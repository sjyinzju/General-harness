//! harness-cli: CLI entry point with automatic managed-temp lifecycle.
//!
//! # Production IPC routing (default)
//!
//! All production commands route through the running Supervisor via
//! Windows Named Pipe IPC. The CLI never opens the database directly
//! for write operations in production mode.
//!
//! | Command | Default Mode | Fallback |
//! |---------|-------------|----------|
//! | task-loop * | IPC | SupervisorUnavailable |
//! | review * | IPC | SupervisorUnavailable |
//! | integration * | IPC | SupervisorUnavailable |
//! | supervisor status | IPC | offline persisted read |
//! | supervisor stop | IPC | lease deactivation |
//! | supervisor run | DIRECT | N/A (creates Supervisor) |
//! | supervisor start | DIRECT | N/A (spawns Supervisor) |
//! | cleanup | DIRECT | N/A (maintenance) |
//!
//! Explicit `--standalone` enables direct DB mode. Write operations
//! in standalone mode are rejected when a healthy Supervisor exists.
//!
//! # invariants
//! - `RunContext::shutdown()` is ALWAYS called (success, failure, cancel, Ctrl+C).
//! - `std::process::exit()` is NEVER called after `RunContext` creation.
//! - No silent standalone/DB fallback for production write commands.
//! - Default CLI never opens the database for writes in IPC mode.

mod commands;
mod ipc_client;

use harness_runtime::db::Database;
use harness_runtime::liveness::RunContext;
use harness_runtime::production_graph::ProductionGraph;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ipc_client::{ClientError, SupervisorClient, DEFAULT_ENDPOINT};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw_args: Vec<String> = std::env::args().collect();
    if raw_args.len() < 2 {
        print_usage();
        return Ok(());
    }

    let standalone = raw_args.contains(&"--standalone".to_string());

    // Filter out --standalone flag to not interfere with position-based dispatch
    let args: Vec<String> = raw_args
        .into_iter()
        .filter(|a| a != "--standalone")
        .collect();

    // ── Resolve repo root ──────────────────────────────────────
    let repo_root = parse_flag(&args, "--repo")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let code_head = parse_flag(&args, "--code-head").unwrap_or("unknown");
    let default_db = repo_root
        .join("target")
        .join("data")
        .join("harness.db")
        .to_string_lossy()
        .to_string();
    let db_path = parse_flag(&args, "--db")
        .map(|s| s.to_string())
        .or_else(|| std::env::var("HARNESS_DB").ok())
        .unwrap_or(default_db);

    // ── Commands that don't need ProductionGraph ─────────────────
    // cleanup and supervisor start don't require the graph
    if args.len() >= 2 && args[1] == "cleanup" {
        return cmd_cleanup(&args, &repo_root, &db_path).await;
    }

    // supervisor start spawns a child process — no graph needed
    if args.len() >= 3 && args[1] == "supervisor" && args[2] == "start" {
        let state_dir = parse_flag(&args, "--state-dir").unwrap_or("default");
        match commands::supervisor::cmd_supervisor_start(&db_path, state_dir).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                eprintln!("error: {e}");
                return Err("supervisor start failed".into());
            }
        }
    }

    // ── Try IPC first for all production commands ──────────────
    // supervisor run is always direct (it IS the supervisor)
    let is_supervisor_run = args.len() >= 3 && args[1] == "supervisor" && args[2] == "run";

    if !standalone && !is_supervisor_run {
        match try_ipc_dispatch(&args).await {
            Ok(true) => return Ok(()),
            Ok(false) => return Err("command failed".into()),
            Err(IpcDispatchResult::SupervisorUnavailable) => {
                // For write commands: hard error, no fallback
                if is_production_write(&args) {
                    eprintln!(
                        "error: Supervisor unavailable — cannot execute write command '{}'",
                        args[1]
                    );
                    eprintln!("Start the supervisor with: harness supervisor start");
                    eprintln!("Or use --standalone for direct database mode.");
                    return Err("SupervisorUnavailable".into());
                }
                // For read commands: fall through to ProductionGraph
                eprintln!("warning: Supervisor unavailable, using offline database (read-only)");
            }
            Err(IpcDispatchResult::CommandFailed(msg)) => {
                eprintln!("error: {msg}");
                return Err(msg.into());
            }
        }
    }

    if standalone {
        eprintln!("╔══════════════════════════════════════════════╗");
        eprintln!("║           STANDALONE MODE                    ║");
        eprintln!("║  Direct database access — no Supervisor IPC ║");
        eprintln!("╚══════════════════════════════════════════════╝");
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
        .or_else(|| {
            // If not explicit, try HARNESS_WORKTREE_ROOT env var
            std::env::var("HARNESS_WORKTREE_ROOT")
                .ok()
                .map(PathBuf::from)
        })
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
            let _ = run_context.shutdown(false).await;
            std::process::exit(1);
        }
    };

    // ── Standalone dual-writer check ───────────────────────────
    if standalone {
        let client = SupervisorClient::new(DEFAULT_ENDPOINT);
        if let Ok(true) = client.ping().await {
            eprintln!("error: StandaloneWriteConflict — a healthy Supervisor is running.");
            eprintln!("Write operations in standalone mode would conflict with the Supervisor.");
            eprintln!("Use default IPC mode (remove --standalone) for production.");
            let _ = run_context.shutdown(false).await;
            std::process::exit(1);
        }
    }

    // ── Run startup janitor ─────────────────────────────────────
    let _startup_result = graph.startup().await;

    // ── Start periodic janitor ──────────────────────────────────
    let janitor_cancel = graph.start_periodic_janitor(Duration::from_secs(300));

    // ── Dispatch with Ctrl+C awareness ──────────────────────────
    let run_succeeded = tokio::select! {
        result = dispatch_direct(&args, &db, &graph, &repo_root, &db_path) => {
            result
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl+C received, initiating graceful shutdown");
            eprintln!("\nInterrupted — shutting down...");
            false
        }
    };

    // ── Cancel periodic janitor ──────────────────────────────────
    janitor_cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
    })
    .await
    .ok();

    // ── Explicit shutdown ───────────────────────────────────────
    let _shutdown_result = graph.shutdown(run_succeeded).await;

    tracing::info!(run_succeeded = run_succeeded, "harness exiting");

    if run_succeeded {
        Ok(())
    } else {
        Err("command failed".into())
    }
}

// ── IPC dispatch ──────────────────────────────────────────────────

enum IpcDispatchResult {
    /// Supervisor not reachable via IPC.
    SupervisorUnavailable,
    /// Command failed with a message.
    CommandFailed(String),
}

impl From<ClientError> for IpcDispatchResult {
    fn from(e: ClientError) -> Self {
        match &e {
            ClientError::Connection(_) => IpcDispatchResult::SupervisorUnavailable,
            ClientError::Serialization(_) => IpcDispatchResult::CommandFailed(e.to_string()),
            ClientError::SupervisorError { code, message } => {
                IpcDispatchResult::CommandFailed(format!("[{code}] {message}"))
            }
        }
    }
}

/// Try to dispatch a CLI command through IPC to the running Supervisor.
async fn try_ipc_dispatch(args: &[String]) -> Result<bool, IpcDispatchResult> {
    let client = SupervisorClient::new(DEFAULT_ENDPOINT);

    // Quick ping to confirm supervisor is reachable
    if !client.ping().await.unwrap_or(false) {
        return Err(IpcDispatchResult::SupervisorUnavailable);
    }

    match args[1].as_str() {
        "task-loop" => try_ipc_task_loop(&client, args).await,
        "review" => try_ipc_review(&client, args).await,
        "integration" => try_ipc_integration(&client, args).await,
        "supervisor" => try_ipc_supervisor(&client, args).await,
        "goal" => try_ipc_goal(&client, args).await,
        _ => Err(IpcDispatchResult::SupervisorUnavailable),
    }
}

async fn try_ipc_task_loop(
    client: &SupervisorClient,
    args: &[String],
) -> Result<bool, IpcDispatchResult> {
    let sub = args.get(2).map(|s| s.as_str()).unwrap_or("");
    match sub {
        "start" => {
            let project = parse_flag(args, "--project").unwrap_or("default");
            let task = parse_flag(args, "--task").unwrap_or("");
            let owner = parse_flag(args, "--owner").unwrap_or("cli");
            let policy = parse_flag(args, "--policy").unwrap_or("{}");
            send_ipc(
                client,
                "task.start",
                serde_json::json!({"project": project, "task": task, "owner": owner, "policy": policy}),
            )
            .await
        }
        "status" => {
            let loop_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "task.status",
                serde_json::json!({"loop_id": loop_id}),
            )
            .await
        }
        "resume" => {
            let loop_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            let owner = parse_flag(args, "--owner").unwrap_or("cli");
            send_ipc(
                client,
                "task.resume",
                serde_json::json!({"loop_id": loop_id, "owner": owner}),
            )
            .await
        }
        "cancel" => {
            let loop_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            let owner = parse_flag(args, "--owner").unwrap_or("cli");
            send_ipc(
                client,
                "task.cancel",
                serde_json::json!({"loop_id": loop_id, "owner": owner}),
            )
            .await
        }
        "inspect" | "dry-run-decision" => {
            let loop_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            let cmd = if sub == "inspect" {
                "task.inspect"
            } else {
                "task.dry_run_decision"
            };
            send_ipc(client, cmd, serde_json::json!({"loop_id": loop_id})).await
        }
        _ => Err(IpcDispatchResult::CommandFailed(format!(
            "unknown subcommand: {sub}"
        ))),
    }
}

async fn try_ipc_review(
    client: &SupervisorClient,
    args: &[String],
) -> Result<bool, IpcDispatchResult> {
    let sub = args.get(2).map(|s| s.as_str()).unwrap_or("");
    match sub {
        "create" => {
            let candidate_id = parse_flag(args, "--candidate")
                .or_else(|| args.get(3).map(|s| s.as_str()))
                .unwrap_or("");
            let reviewer = parse_flag(args, "--reviewer").unwrap_or("default-reviewer");
            send_ipc(
                client,
                "review.create",
                serde_json::json!({"candidate_id": candidate_id, "reviewer": reviewer}),
            )
            .await
        }
        "show" => {
            let review_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "review.show",
                serde_json::json!({"review_id": review_id}),
            )
            .await
        }
        "run" => {
            let review_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "review.run",
                serde_json::json!({"review_id": review_id}),
            )
            .await
        }
        "list" => {
            let state = parse_flag(args, "--state");
            send_ipc(client, "review.list", serde_json::json!({"state": state})).await
        }
        _ => Err(IpcDispatchResult::CommandFailed(format!(
            "unknown subcommand: {sub}"
        ))),
    }
}

async fn try_ipc_integration(
    client: &SupervisorClient,
    args: &[String],
) -> Result<bool, IpcDispatchResult> {
    let sub = args.get(2).map(|s| s.as_str()).unwrap_or("");
    match sub {
        "enqueue" => {
            let candidate_id = parse_flag(args, "--candidate").unwrap_or("");
            let repo_id = parse_flag(args, "--repo-id").unwrap_or("default");
            let target_ref = parse_flag(args, "--target-ref").unwrap_or("refs/heads/main");
            let priority = parse_flag(args, "--priority")
                .unwrap_or("0")
                .parse::<i64>()
                .unwrap_or(0);
            send_ipc(
                client,
                "integration.enqueue",
                serde_json::json!({
                    "candidate_id": candidate_id,
                    "repo_id": repo_id,
                    "target_ref": target_ref,
                    "priority": priority,
                }),
            )
            .await
        }
        "run-next" => {
            let repo_id = parse_flag(args, "--repo-id").unwrap_or("default");
            let target_ref = parse_flag(args, "--target-ref").unwrap_or("refs/heads/main");
            send_ipc(
                client,
                "integration.run_next",
                serde_json::json!({"repo_id": repo_id, "target_ref": target_ref}),
            )
            .await
        }
        "show" => {
            let id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "integration.show",
                serde_json::json!({"integration_id": id}),
            )
            .await
        }
        "list" => send_ipc(client, "integration.list", serde_json::json!({})).await,
        "cancel" => {
            let id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "integration.cancel",
                serde_json::json!({"integration_id": id}),
            )
            .await
        }
        "recover" => send_ipc(client, "integration.recover", serde_json::json!({})).await,
        _ => Err(IpcDispatchResult::CommandFailed(format!(
            "unknown subcommand: {sub}"
        ))),
    }
}

async fn try_ipc_supervisor(
    client: &SupervisorClient,
    args: &[String],
) -> Result<bool, IpcDispatchResult> {
    let sub = args.get(2).map(|s| s.as_str()).unwrap_or("");
    match sub {
        "status" => send_ipc(client, "supervisor.status", serde_json::json!({})).await,
        "stop" => send_ipc(client, "supervisor.stop", serde_json::json!({})).await,
        "diagnostics" => send_ipc(client, "diagnostics", serde_json::json!({})).await,
        // "run" and "start" are never dispatched through IPC
        _ => Err(IpcDispatchResult::CommandFailed(format!(
            "unknown subcommand: {sub}"
        ))),
    }
}

/// Send an IPC request and render the response.
async fn send_ipc(
    client: &SupervisorClient,
    command: &str,
    payload: serde_json::Value,
) -> Result<bool, IpcDispatchResult> {
    let response = client.send_request(command, payload).await?;

    if let Some(ref error) = response.error {
        eprintln!("error: [{}] {}", error.code, error.message);
        return Ok(false);
    }

    if let Some(ref p) = response.payload {
        // Pretty-print if --json flag is not set
        println!("{}", serde_json::to_string_pretty(p).unwrap_or_default());
    }

    Ok(true)
}

async fn try_ipc_goal(
    client: &SupervisorClient,
    args: &[String],
) -> Result<bool, IpcDispatchResult> {
    let sub = args.get(2).map(|s| s.as_str()).unwrap_or("");
    match sub {
        "create" => {
            // Support --spec-file for reliable JSON passing (avoids shell quoting)
            let spec_val = if let Some(path) = parse_flag(args, "--spec-file") {
                match std::fs::read_to_string(path) {
                    Ok(contents) => match serde_json::from_str::<serde_json::Value>(&contents) {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("error: invalid JSON in spec file: {e}");
                            return Err(IpcDispatchResult::CommandFailed(format!(
                                "invalid spec file: {e}"
                            )));
                        }
                    },
                    Err(e) => {
                        eprintln!("error: cannot read spec file: {e}");
                        return Err(IpcDispatchResult::CommandFailed(format!(
                            "cannot read spec file: {e}"
                        )));
                    }
                }
            } else {
                let spec = parse_flag(args, "--spec").unwrap_or("{}");
                match serde_json::from_str::<serde_json::Value>(spec) {
                    Ok(v) => v,
                    Err(_) => serde_json::json!({ "goal_spec_str": spec }),
                }
            };
            send_ipc(client, "goal.create", spec_val).await
        }
        "start" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "goal.start",
                serde_json::json!({ "goal_id": goal_id }),
            )
            .await
        }
        "show" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "goal.show",
                serde_json::json!({ "goal_id": goal_id }),
            )
            .await
        }
        "list" => {
            let state = parse_flag(args, "--state");
            let mut payload = serde_json::json!({});
            if let Some(s) = state {
                payload["state"] = serde_json::json!(s);
            }
            send_ipc(client, "goal.list", payload).await
        }
        "status" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "goal.status",
                serde_json::json!({ "goal_id": goal_id }),
            )
            .await
        }
        "pause" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "goal.pause",
                serde_json::json!({ "goal_id": goal_id }),
            )
            .await
        }
        "resume" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "goal.resume",
                serde_json::json!({ "goal_id": goal_id }),
            )
            .await
        }
        "cancel" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "goal.cancel",
                serde_json::json!({ "goal_id": goal_id }),
            )
            .await
        }
        "replan" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            let reason = parse_flag(args, "--reason").unwrap_or("");
            send_ipc(
                client,
                "goal.replan",
                serde_json::json!({ "goal_id": goal_id, "reason": reason }),
            )
            .await
        }
        "approvals" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "goal.approvals",
                serde_json::json!({ "goal_id": goal_id }),
            )
            .await
        }
        "approve" => {
            let approval_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            send_ipc(
                client,
                "goal.approve",
                serde_json::json!({ "approval_id": approval_id }),
            )
            .await
        }
        "reject" => {
            let approval_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            let reason = parse_flag(args, "--reason").unwrap_or("");
            send_ipc(
                client,
                "goal.reject",
                serde_json::json!({ "approval_id": approval_id, "reason": reason }),
            )
            .await
        }
        "answer" => {
            let approval_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            let value = parse_flag(args, "--value").unwrap_or("");
            send_ipc(
                client,
                "goal.answer",
                serde_json::json!({ "approval_id": approval_id, "value": value }),
            )
            .await
        }
        "events" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            let after =
                parse_flag(args, "--after-sequence").map(|s: &str| s.parse::<i64>().unwrap_or(0));
            send_ipc(
                client,
                "goal.events",
                serde_json::json!({ "goal_id": goal_id, "after_sequence": after.unwrap_or(0) }),
            )
            .await
        }
        _ => Err(IpcDispatchResult::CommandFailed(format!(
            "unknown goal subcommand: {sub}"
        ))),
    }
}

/// Determine if a CLI command is a production write (side-effect) command.
fn is_production_write(args: &[String]) -> bool {
    if args.len() < 3 {
        return false;
    }
    match args[1].as_str() {
        "task-loop" => matches!(args[2].as_str(), "start" | "resume" | "cancel"),
        "review" => matches!(args[2].as_str(), "create" | "run"),
        "integration" => matches!(
            args[2].as_str(),
            "enqueue" | "run-next" | "cancel" | "recover"
        ),
        "supervisor" => matches!(args[2].as_str(), "stop"),
        "goal" => matches!(
            args[2].as_str(),
            "create"
                | "start"
                | "pause"
                | "resume"
                | "cancel"
                | "replan"
                | "approve"
                | "reject"
                | "answer"
        ),
        _ => false,
    }
}

// ── Direct dispatch (standalone / supervisor run / fallback) ──────

async fn dispatch_direct(
    args: &[String],
    db: &Database,
    graph: &ProductionGraph,
    repo_path: &Path,
    db_path: &str,
) -> bool {
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
        "integration" => {
            if args.len() < 3 {
                eprintln!("error: missing integration subcommand");
                false
            } else {
                dispatch_integration(args, db, repo_path).await
            }
        }
        "goal" => {
            eprintln!("error: goal commands require Supervisor IPC");
            false
        }
        "supervisor" => {
            if args.len() < 3 {
                eprintln!("error: missing supervisor subcommand");
                false
            } else {
                dispatch_supervisor(args, db, db_path, graph).await
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

async fn dispatch_integration(args: &[String], db: &Database, repo_path: &Path) -> bool {
    let integration_root = repo_path.join("target").join("harness-integration");
    match args[2].as_str() {
        "enqueue" => {
            let candidate_id = match parse_flag(args, "--candidate") {
                Some(c) => c,
                None => {
                    eprintln!("error: --candidate <candidate-id> required");
                    return false;
                }
            };
            let repo_id = parse_flag(args, "--repo-id").unwrap_or("default");
            let target_ref = parse_flag(args, "--target-ref").unwrap_or("refs/heads/main");
            let priority: i32 = parse_flag(args, "--priority")
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            match commands::integration::cmd_integration_enqueue(
                db,
                candidate_id,
                repo_id,
                target_ref,
                priority,
                repo_path,
            )
            .await
            {
                Ok(json_output) => {
                    println!("{json_output}");
                    true
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "run-next" => {
            let repo_id = parse_flag(args, "--repo-id").unwrap_or("default");
            let target_ref = parse_flag(args, "--target-ref").unwrap_or("refs/heads/main");
            match commands::integration::cmd_integration_run_next(
                db,
                repo_id,
                target_ref,
                repo_path,
                &integration_root,
            )
            .await
            {
                Ok(json_output) => {
                    println!("{json_output}");
                    true
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "show" => {
            let json = args.contains(&"--json".to_string());
            let id = match args.get(3) {
                Some(i) => i,
                None => {
                    eprintln!("error: integration-id required");
                    return false;
                }
            };
            match commands::integration::cmd_integration_show(db, id, json).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "list" => {
            let json = args.contains(&"--json".to_string());
            match commands::integration::cmd_integration_list(db, json).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "cancel" => {
            let id = match args.get(3) {
                Some(i) => i,
                None => {
                    eprintln!("error: integration-id required");
                    return false;
                }
            };
            match commands::integration::cmd_integration_cancel(db, id).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "recover" => {
            let json = args.contains(&"--json".to_string());
            match commands::integration::cmd_integration_recover(
                db,
                repo_path,
                &integration_root,
                json,
            )
            .await
            {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        _ => {
            eprintln!("error: unknown integration subcommand: {}", args[2]);
            eprintln!(
                "Usage: harness integration <enqueue|run-next|show|list|cancel|recover> [args]"
            );
            false
        }
    }
}

fn print_usage() {
    println!("harness v0.1.0 — task engineering harness");
    println!("Usage:");
    println!("  harness task-loop start --project <id> --task <id> [--owner <id>] [--policy <json>] [--repo <path>] [--worktree-root <path>] [--code-head <sha>] [--standalone]");
    println!("  harness task-loop status <loop-id> [--repo <path>] [--standalone]");
    println!("  harness task-loop resume <loop-id> [--owner <id>] [--repo <path>] [--worktree-root <path>] [--standalone]");
    println!("  harness task-loop cancel <loop-id> [--owner <id>] [--repo <path>] [--standalone]");
    println!("  harness task-loop inspect <loop-id> [--json] [--repo <path>] [--standalone]");
    println!("  harness task-loop dry-run-decision <loop-id> [--repo <path>] [--standalone]");
    println!("  harness review create <candidate-id> [--reviewer <profile-id>] [--repo <path>] [--standalone]");
    println!("  harness review run <review-id> [--repo <path>] [--standalone]");
    println!("  harness review show <review-id> [--json] [--repo <path>] [--standalone]");
    println!("  harness review list [--state <state>] [--json] [--repo <path>] [--standalone]");
    println!("  harness integration enqueue --candidate <id> [--repo-id <id>] [--target-ref <ref>] [--priority <n>] [--repo <path>] [--standalone]");
    println!("  harness integration run-next [--repo-id <id>] [--target-ref <ref>] [--repo <path>] [--standalone]");
    println!("  harness integration show <id> [--json] [--repo <path>] [--standalone]");
    println!("  harness integration list [--json] [--repo <path>] [--standalone]");
    println!("  harness integration cancel <id> [--repo <path>] [--standalone]");
    println!("  harness integration recover [--repo <path>] [--standalone]");
    println!("  harness supervisor run [--state-dir <id>] [--repo <path>]");
    println!("  harness supervisor start [--state-dir <id>] [--repo <path>]");
    println!("  harness supervisor status [--state-dir <id>] [--json] [--repo <path>]");
    println!("  harness supervisor stop [--state-dir <id>] [--repo <path>]");
    println!("  harness cleanup [--dry-run|--apply] [--repo <path>]");
    println!();
    println!("Modes:");
    println!("  Default:  IPC via running Supervisor (production)");
    println!("  --standalone: Direct database access (development/maintenance)");
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
                None => match args.get(3) {
                    Some(c) if !c.starts_with("--") => c.as_str(),
                    _ => {
                        eprintln!("error: --candidate <id> required (or use positional)");
                        return false;
                    }
                },
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

async fn dispatch_supervisor(
    args: &[String],
    db: &Database,
    db_path: &str,
    graph: &ProductionGraph,
) -> bool {
    let state_dir = parse_flag(args, "--state-dir").unwrap_or("default");
    match args[2].as_str() {
        "run" => match commands::supervisor::cmd_supervisor_run(
            db,
            state_dir,
            graph.supervisor_services.clone(),
        )
        .await
        {
            Ok(()) => true,
            Err(e) => {
                eprintln!("error: {e}");
                false
            }
        },
        "start" => match commands::supervisor::cmd_supervisor_start(db_path, state_dir).await {
            Ok(()) => true,
            Err(e) => {
                eprintln!("error: {e}");
                false
            }
        },
        "status" => {
            let json = args.contains(&"--json".to_string());
            match commands::supervisor::cmd_supervisor_status(db, state_dir, json).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "stop" => match commands::supervisor::cmd_supervisor_stop(db, state_dir).await {
            Ok(()) => true,
            Err(e) => {
                eprintln!("error: {e}");
                false
            }
        },
        _ => {
            eprintln!("error: unknown supervisor subcommand: {}", args[2]);
            eprintln!("Usage: harness supervisor <run|start|status|stop> [--state-dir <id>]");
            false
        }
    }
}

fn parse_flag<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    let idx = args.iter().position(|a| a == flag)?;
    args.get(idx + 1).map(|s| s.as_str())
}
