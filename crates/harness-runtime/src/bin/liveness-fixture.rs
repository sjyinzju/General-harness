//! Liveness test fixture — a standalone binary that creates managed
//! temp directories with ownership markers and waits for signals.
//!
//! Used by G8 (cross-process crash recovery) and G10 (concurrent
//! active-run protection) integration tests.
//!
//! Commands:
//!   create-temp <sandbox> <run_id> <code_head> [--wait-forever]
//!     Creates a managed temp dir with marker, optionally waits.
//!   ready-signal <file>
//!     Touches a file to signal readiness.

use std::path::PathBuf;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: liveness-fixture <command> [args...]");
        eprintln!("  create-temp <sandbox> <run_id> <code_head> [--wait-forever]");
        eprintln!("  ready-signal <file>");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "create-temp" => cmd_create_temp(&args),
        "ready-signal" => cmd_ready_signal(&args),
        other => {
            eprintln!("unknown command: {other}");
            std::process::exit(1);
        }
    }
}

fn cmd_create_temp(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if args.len() < 5 {
        eprintln!("create-temp <sandbox> <run_id> <code_head> [--wait-forever]");
        std::process::exit(1);
    }
    let sandbox = PathBuf::from(&args[2]);
    let run_id = &args[3];
    let code_head = &args[4];
    let wait_forever = args.get(5).map(|s| s == "--wait-forever").unwrap_or(false);

    let managed_root = sandbox.join("harness-temp");
    std::fs::create_dir_all(&managed_root)?;

    let dir = managed_root.join(run_id);
    std::fs::create_dir_all(&dir)?;

    // Write ownership marker.
    let now = chrono::Utc::now().to_rfc3339();
    let marker = serde_json::json!({
        "schema_version": 1,
        "kind": "harness-managed-temp",
        "run_id": run_id,
        "owner_pid": std::process::id(),
        "owner_process_created_at": now,
        "created_at": now,
        "code_head": code_head,
        "state": "active",
    });
    let marker_path = dir.join(".harness-owned.json");
    let tmp = dir.join(".harness-owned.json.tmp");
    std::fs::write(&tmp, marker.to_string())?;
    std::fs::rename(&tmp, &marker_path)?;

    // Touch a file to confirm creation.
    std::fs::write(dir.join("fixture-data.txt"), b"test data from fixture")?;

    println!("CREATED {}", dir.display());
    println!("PID {}", std::process::id());

    if wait_forever {
        println!("WAITING (send SIGTERM or kill process)");
        // Wait indefinitely — the test harness will kill this process.
        loop {
            std::thread::sleep(Duration::from_secs(10));
        }
    }

    Ok(())
}

fn cmd_ready_signal(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if args.len() < 3 {
        eprintln!("ready-signal <file>");
        std::process::exit(1);
    }
    let signal_file = PathBuf::from(&args[2]);
    if let Some(parent) = signal_file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&signal_file, format!("{}", std::process::id()))?;
    println!("SIGNALED {}", signal_file.display());
    Ok(())
}
