//! Supervisor CLI — lifecycle commands for the harness supervisor daemon.
//!
//! In I6.1, these commands work directly (no IPC required).
//! In I6.2+, the default production path will route through IPC.

use harness_core::contracts::supervisor::{SupervisorConfig, SupervisorState};
use harness_runtime::db::Database;
use harness_runtime::supervisor::repo::SupervisorRepo;
use harness_runtime::supervisor::Supervisor;

/// Run the supervisor in the foreground. Blocks until stopped or error.
pub async fn cmd_supervisor_run(
    db: &Database,
    state_directory_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = SupervisorConfig {
        state_directory_id: state_directory_id.to_string(),
        ..SupervisorConfig::default()
    };

    let supervisor = Supervisor::new(config, db.pool.clone());

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

    let child = cmd.spawn().map_err(|e| format!("failed to start supervisor: {e}"))?;
    println!(
        "Supervisor started (PID: {}) for state directory: {}",
        child.id(),
        state_directory_id
    );
    println!("Use 'harness supervisor status' to check health");

    Ok(())
}

/// Show supervisor status from the database.
pub async fn cmd_supervisor_status(
    db: &Database,
    state_directory_id: &str,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repo = SupervisorRepo::new(db.pool.clone());

    match repo.get_active_instance_for_dir(state_directory_id).await {
        Ok(Some(instance)) => {
            let lease = repo.get_active_lease(state_directory_id).await.ok().flatten();

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
                });
                println!("{}", serde_json::to_string_pretty(&status)?);
            } else {
                println!("Supervisor Status:");
                println!("  Instance ID:       {}", instance.instance_id);
                println!("  State:             {}", instance.state);
                println!("  PID:               {}", instance.pid);
                println!(
                    "  Process Started:   {}",
                    instance.process_started_at.to_rfc3339()
                );
                println!("  Fencing Token:     {}", instance.fencing_token);
                println!("  Started At:        {}", instance.started_at.to_rfc3339());
                println!("  Last Heartbeat:    {}", instance.heartbeat_at.to_rfc3339());
                println!(
                    "  Lease Expires:     {}",
                    instance.lease_expires_at.to_rfc3339()
                );
                println!(
                    "  Lease Active:      {}",
                    lease.map(|l| l.is_active == 1).unwrap_or(false)
                );
                println!("  Protocol Version:  {}", instance.protocol_version);
                println!("  Binary Version:    {}", instance.binary_version);
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

/// Request the supervisor to stop gracefully.
/// In I6.1, this updates the lease to expire immediately.
pub async fn cmd_supervisor_stop(
    db: &Database,
    state_directory_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
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

            // Deactivate the lease to trigger graceful stop on next heartbeat
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
