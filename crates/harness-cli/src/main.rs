//! harness-cli: CLI entry point and interactive TUI.
//! Depends on harness-runtime, harness-adapters, ratatui, crossterm.

mod commands;

use harness_runtime::db::Database;
use harness_runtime::production_graph::ProductionGraph;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("harness v0.1.0 — task engineering harness");
        println!("Usage:");
        println!(
            "  harness task-loop start --project <id> --task <id> [--owner <id>] [--policy <json>] [--repo <path>] [--worktree-root <path>]"
        );
        println!("  harness task-loop status <loop-id>");
        println!("  harness task-loop resume <loop-id> [--owner <id>] [--repo <path>] [--worktree-root <path>]");
        println!("  harness task-loop cancel <loop-id> [--owner <id>]");
        println!("  harness task-loop inspect <loop-id> [--json]");
        println!("  harness task-loop dry-run-decision <loop-id>");
        return Ok(());
    }

    match args[1].as_str() {
        "task-loop" => {
            if args.len() < 3 {
                eprintln!("error: missing task-loop subcommand");
                return Ok(());
            }
            let db_path = std::env::var("HARNESS_DB").unwrap_or_else(|_| "harness.db".to_string());
            let db = Database::open(&PathBuf::from(&db_path)).await?;

            // Check for production I4 wiring flags.
            let repo = parse_flag(&args, "--repo");
            let worktree_root = parse_flag(&args, "--worktree-root");

            match args[2].as_str() {
                "start" => {
                    let project = parse_flag(&args, "--project").unwrap_or("default");
                    let task = parse_flag(&args, "--task").ok_or("--task required")?;
                    let owner = parse_flag(&args, "--owner").unwrap_or("cli");
                    let policy = parse_flag(&args, "--policy").unwrap_or("{}");
                    let graph = build_graph_if_repo(&db, repo, worktree_root)?;
                    commands::task_loop::cmd_start(
                        &db,
                        graph.as_ref(),
                        project,
                        task,
                        owner,
                        policy,
                    )
                    .await?;
                }
                "status" => {
                    let loop_id = args.get(3).ok_or("loop-id required")?;
                    let graph = build_graph_if_repo(&db, repo, worktree_root)?;
                    commands::task_loop::cmd_status(&db, graph.as_ref(), loop_id).await?;
                }
                "resume" => {
                    let loop_id = args.get(3).ok_or("loop-id required")?;
                    let owner = parse_flag(&args, "--owner").unwrap_or("cli");
                    let graph = build_graph_if_repo(&db, repo, worktree_root)?;
                    commands::task_loop::cmd_resume(&db, graph.as_ref(), loop_id, owner).await?;
                }
                "cancel" => {
                    let loop_id = args.get(3).ok_or("loop-id required")?;
                    let owner = parse_flag(&args, "--owner").unwrap_or("cli");
                    let graph = build_graph_if_repo(&db, repo, worktree_root)?;
                    commands::task_loop::cmd_cancel(&db, graph.as_ref(), loop_id, owner).await?;
                }
                "inspect" => {
                    let loop_id = args.get(3).ok_or("loop-id required")?;
                    let graph = build_graph_if_repo(&db, repo, worktree_root)?;
                    if args.contains(&"--json".to_string()) {
                        commands::task_loop::cmd_inspect_json(&db, graph.as_ref(), loop_id).await?;
                    } else {
                        commands::task_loop::cmd_status(&db, graph.as_ref(), loop_id).await?;
                    }
                }
                "dry-run-decision" => {
                    let loop_id = args.get(3).ok_or("loop-id required")?;
                    let graph = build_graph_if_repo(&db, repo, worktree_root)?;
                    commands::task_loop::cmd_dry_run_decision(&db, graph.as_ref(), loop_id).await?;
                }
                other => eprintln!("error: unknown subcommand: {other}"),
            }
        }
        _ => println!("harness v0.1.0 — unknown command: {}", args[1]),
    }
    Ok(())
}

/// Build a production service graph when `--repo` is provided.
/// Without `--repo`, returns `None` and the CLI falls back to the
/// simple (direct-SQL) path — useful for read-only commands or
/// environments where a git repository is not available.
fn build_graph_if_repo(
    db: &Database,
    repo: Option<&str>,
    worktree_root: Option<&str>,
) -> Result<Option<ProductionGraph>, Box<dyn std::error::Error>> {
    match (repo, worktree_root) {
        (Some(repo_path), Some(wt_root)) => {
            let graph = ProductionGraph::build(
                db.pool.clone(),
                &PathBuf::from(wt_root),
                &PathBuf::from(repo_path),
            )?;
            Ok(Some(graph))
        }
        (Some(_), None) => {
            eprintln!(
                "warning: --repo provided without --worktree-root; \
                 using simple path (no real I4 dispatch)"
            );
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn parse_flag<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    let idx = args.iter().position(|a| a == flag)?;
    args.get(idx + 1).map(|s| s.as_str())
}
