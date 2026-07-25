//! Supervisor CLI — lifecycle commands for the harness supervisor daemon.
//!
//! Production path (default): routes through IPC to a running Supervisor.
//! Supervisor run/start: creates and manages the Supervisor daemon process.

use harness_core::contracts::supervisor::{SupervisorConfig, SupervisorState};
use harness_runtime::db::Database;
use harness_runtime::supervisor::repo::SupervisorRepo;
use harness_runtime::supervisor::{Supervisor, SupervisorServices};

/// Run the supervisor in the foreground. Blocks until stopped or error.
pub async fn cmd_supervisor_run(
    db: &Database,
    state_directory_id: &str,
    services: SupervisorServices,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = SupervisorConfig {
        state_directory_id: state_directory_id.to_string(),
        ..SupervisorConfig::default()
    };

    let supervisor = Supervisor::new(config, db.pool.clone(), services);

    println!("Starting supervisor for state directory: {state_directory_id}");
    println!("Instance ID will be assigned at startup");

    match supervisor.run(state_directory_id).await {
        Ok(()) => {
            println!("Supervisor exited normally.");
            Ok(())
        }
        Err(e) => {
            eprintln!("Supervisor failed: {e}");
            Err(e.into())
        }
    }
}

/// Start the supervisor as a background process.
/// Spawns the same binary with `supervisor run` as a detached child.
pub async fn cmd_supervisor_start(
    db_path: &str,
    state_directory_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if a supervisor is already running for this directory
    let db = Database::open(&std::path::PathBuf::from(db_path)).await?;
    let repo = SupervisorRepo::new(db.pool.clone());
    if let Ok(Some(instance)) = repo.get_active_instance_for_dir(state_directory_id).await {
        if instance.state.is_active() || instance.state == SupervisorState::Recovering {
            return Err(format!(
                "Supervisor {} is already running (state: {})",
                instance.instance_id, instance.state
            )
            .into());
        }
    }

    // Spawn self as a detached child process
    let exe = std::env::current_exe().map_err(|e| format!("cannot find executable: {e}"))?;
    let mut cmd = std::process::Command::new(&exe);
    cmd.arg("supervisor")
        .arg("run")
        .arg("--state-dir")
        .arg(state_directory_id)
        .arg("--db")
        .arg(db_path);

    // On Windows, use CREATE_NEW_PROCESS_GROUP for detachment
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x00000200;
        const DETACHED_PROCESS: u32 = 0x00000008;
        cmd.creation_flags(CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS);
    }

    let child = cmd
        .spawn()
        .map_err(|e| format!("failed to start supervisor: {e}"))?;
    println!(
        "Supervisor started (PID: {}) for state directory: {}",
        child.id(),
        state_directory_id
    );
    println!("Use 'harness supervisor status' to check health");

    Ok(())
}

/// Show supervisor status — tries IPC first, falls back to direct DB read.
pub async fn cmd_supervisor_status(
    db: &Database,
    state_directory_id: &str,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Try IPC first
    let ipc_result = try_ipc_status(state_directory_id).await;

    match ipc_result {
        Ok(Some(status_json)) => {
            if json {
                println!("{}", serde_json::to_string_pretty(&status_json)?);
            } else {
                println!("Supervisor Status (via IPC):");
                if let Some(inst) = status_json.get("instance_id").and_then(|v| v.as_str()) {
                    println!("  Instance ID:       {inst}");
                }
                if let Some(state) = status_json.get("state").and_then(|v| v.as_str()) {
                    println!("  State:             {state}");
                }
                if let Some(pid) = status_json.get("pid") {
                    println!("  PID:               {pid}");
                }
                if let Some(token) = status_json.get("fencing_token") {
                    println!("  Fencing Token:     {token}");
                }
                if let Some(ts) = status_json.get("heartbeat_at").and_then(|v| v.as_str()) {
                    println!("  Last Heartbeat:    {ts}");
                }
            }
            return Ok(());
        }
        Ok(None) => {
            // No supervisor reachable via IPC — fall back to DB
        }
        Err(_) => {
            // IPC error — fall back to DB
        }
    }

    // Fallback: direct database read
    let repo = SupervisorRepo::new(db.pool.clone());

    match repo.get_active_instance_for_dir(state_directory_id).await {
        Ok(Some(instance)) => {
            let lease = repo
                .get_active_lease(state_directory_id)
                .await
                .ok()
                .flatten();

            if json {
                let status = serde_json::json!({
                    "instance_id": instance.instance_id.0,
                    "state": instance.state.to_string(),
                    "pid": instance.pid,
                    "process_started_at": instance.process_started_at.to_rfc3339(),
                    "fencing_token": instance.fencing_token,
                    "started_at": instance.started_at.to_rfc3339(),
                    "heartbeat_at": instance.heartbeat_at.to_rfc3339(),
                    "lease_expires_at": instance.lease_expires_at.to_rfc3339(),
                    "protocol_version": instance.protocol_version,
                    "binary_version": instance.binary_version,
                    "lease_active": lease.map(|l| l.is_active == 1).unwrap_or(false),
                    "source": "database (offline)",
                });
                println!("{}", serde_json::to_string_pretty(&status)?);
            } else {
                println!("Supervisor Status (via database — offline):");
                println!("  Instance ID:       {}", instance.instance_id);
                println!("  State:             {}", instance.state);
                println!("  PID:               {}", instance.pid);
                println!(
                    "  Process Started:   {}",
                    instance.process_started_at.to_rfc3339()
                );
                println!("  Fencing Token:     {}", instance.fencing_token);
                println!("  Started At:        {}", instance.started_at.to_rfc3339());
                println!(
                    "  Last Heartbeat:    {}",
                    instance.heartbeat_at.to_rfc3339()
                );
                println!(
                    "  Lease Expires:     {}",
                    instance.lease_expires_at.to_rfc3339()
                );
                println!(
                    "  Lease Active:      {}",
                    lease.map(|l| l.is_active == 1).unwrap_or(false)
                );
            }
            Ok(())
        }
        Ok(None) => {
            if json {
                println!("{{\"status\": \"no_supervisor_running\"}}");
            } else {
                println!("No supervisor running for state directory: {state_directory_id}");
            }
            Ok(())
        }
        Err(e) => Err(format!("Failed to query supervisor status: {e}").into()),
    }
}

/// Try to get supervisor status via IPC.
async fn try_ipc_status(
    _state_directory_id: &str,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    // Attempt IPC connection to the supervisor
    match crate::ipc_client::SupervisorClient::new(crate::ipc_client::DEFAULT_ENDPOINT)
        .send_request("supervisor.status", serde_json::json!({}))
        .await
    {
        Ok(resp) => {
            if let Some(payload) = resp.payload {
                Ok(Some(payload))
            } else {
                Ok(None)
            }
        }
        Err(_) => Ok(None), // Supervisor not reachable
    }
}

/// Request the supervisor to stop gracefully.
/// Tries IPC first, falls back to direct lease deactivation.
pub async fn cmd_supervisor_stop(
    db: &Database,
    state_directory_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Try IPC first
    match crate::ipc_client::SupervisorClient::new(crate::ipc_client::DEFAULT_ENDPOINT)
        .send_request("supervisor.stop", serde_json::json!({}))
        .await
    {
        Ok(resp) => {
            if let Some(payload) = resp.payload {
                if payload
                    .get("acknowledged")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    println!("Stop request acknowledged by supervisor.");
                    return Ok(());
                }
            }
        }
        Err(_) => {
            // IPC not available — fall back to direct DB
        }
    }

    // Fallback: direct database
    let repo = SupervisorRepo::new(db.pool.clone());

    match repo.get_active_instance_for_dir(state_directory_id).await {
        Ok(Some(instance)) => {
            if instance.state.is_terminal() {
                println!(
                    "Supervisor {} is already in terminal state: {}",
                    instance.instance_id, instance.state
                );
                return Ok(());
            }

            repo.force_deactivate_lease(state_directory_id).await?;
            println!(
                "Stop request sent to supervisor {} (PID: {}).",
                instance.instance_id, instance.pid
            );
            println!("The supervisor will drain and stop within a few seconds.");
            Ok(())
        }
        Ok(None) => {
            println!("No supervisor running for state directory: {state_directory_id}");
            Ok(())
        }
        Err(e) => Err(format!("Failed to stop supervisor: {e}").into()),
    }
}
