//! Process Fixture — test binary for ProcessManager validation.
//! Build: cargo build --bin process-fixture
//! Run:  process-fixture <mode> [args...]

use std::env;
use std::io::{self, Read, Write};
use std::process;
use std::thread;
use std::time::Duration;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let mode = args.first().map(|s| s.as_str()).unwrap_or("print_stdout");

    match mode {
        "print_stdout" => println!("stdout: hello"),
        "print_stderr" => eprintln!("stderr: hello"),
        "print_both" => {
            println!("stdout");
            eprintln!("stderr");
        }
        "exit_with_code" => {
            let code: i32 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            process::exit(code);
        }
        "sleep" => {
            let secs: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(5);
            thread::sleep(Duration::from_secs(secs));
        }
        "read_stdin" => {
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf).ok();
            println!("stdin: {buf}");
        }
        "read_stdin_then_exit" => {
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf).ok();
            println!("received: {buf}");
            process::exit(0);
        }
        "print_cwd" => println!("{}", env::current_dir().unwrap().display()),
        "print_env" => {
            if let Some(key) = args.get(1) {
                println!(
                    "{}={}",
                    key,
                    env::var(key).unwrap_or_else(|_| "<unset>".into())
                );
            }
        }
        "flood_stdout" => {
            let count: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
            for i in 0..count {
                println!("stdout line {i}");
            }
        }
        "flood_stderr" => {
            let count: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
            for i in 0..count {
                eprintln!("stderr line {i}");
            }
        }
        "flood_both" => {
            let count: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
            for i in 0..count {
                println!("out {i}");
                eprintln!("err {i}");
            }
        }
        "invalid_utf8" => {
            let _ = io::stdout().write_all(&[0xFF, 0xFE, 0x00]);
        }
        "spawn_child" => {
            let exe = env::current_exe().unwrap();
            let child_pid = process::Command::new(&exe).arg("sleep").arg("10").spawn();
            let pid = child_pid.unwrap().id();
            // Write grandchild PID to grandchild.txt in the readiness dir.
            // Uses READY_DIR if set, otherwise current directory (set by
            // ProcessManager as the working_directory).
            // Best-effort: write grandchild PID to grandchild.txt for
            // the deterministic readiness protocol. Ignore errors for
            // backward compat with spawn_grandchild mode.
            if let Ok(rd) = env::var("READY_DIR") {
                let _ = std::fs::write(format!("{rd}/grandchild.txt"), pid.to_string());
            } else if let Ok(cwd) = env::current_dir() {
                let _ = std::fs::write(
                    cwd.join("grandchild.txt"),
                    pid.to_string(),
                );
            }
        }
        "spawn_grandchild" => {
            let exe = env::current_exe().unwrap();
            let child = process::Command::new(&exe)
                .arg("spawn_child")
                .spawn()
                .unwrap();
            let _ = child.wait_with_output();
        }
        "spawn_tree_and_sleep" => {
            // Write startup marker IMMEDIATELY to verify process starts.
            let start_marker = env::var("READY_DIR").unwrap_or_else(|_| {
                env::current_dir()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default()
            });
            let _ = std::fs::write(
                format!("{start_marker}/started.txt"),
                format!("pid={} time={:?}", process::id(), std::time::Instant::now()),
            );
            // Deterministic readiness via JSON file with atomic rename.
            // READY_DIR env var → ready.json.tmp → fsync → rename → ready.json.
            // Contains root/child/grandchild PIDs.
            let exe = env::current_exe().unwrap();
            let ready_dir = env::var("READY_DIR").unwrap_or_else(|_| {
                env::current_dir()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|_| env::temp_dir().to_string_lossy().to_string())
            });
            let root_pid = process::id();

            // Give ProcessManager a bounded window to complete Job Object
            // assignment before this process spawns children. Without this,
            // a child created before AssignProcessToJobObject completes can
            // escape the Job — the Job Object only auto-inherits children
            // created AFTER the parent is assigned. 50 ms is ~50× the normal
            // FFI latency for CreateJobObjectW + SetInformationJobObject +
            // AssignProcessToJobObject combined.
            thread::sleep(Duration::from_millis(50));

            // Spawn child. Child spawns grandchild (sleep 10), writes
            // grandchild PID to READY_DIR/grandchild.txt, prints
            // "child_pid=<grandchild>" to inherited stdout, then exits.
            let mut child = process::Command::new(&exe)
                .arg("spawn_child")
                .spawn()
                .unwrap();
            let child_pid = child.id();
            child.wait().unwrap();

            // Read grandchild PID from file written by spawn_child.
            let gc_path = format!("{ready_dir}/grandchild.txt");
            let grandchild_pid = std::fs::read_to_string(&gc_path)
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or(0);

            // Write ready.json with atomic rename.
            let ready_json = format!(
                concat!(
                    r#"{{"run_id":"{}","root_pid":{},"#,
                    r#""child_pid":{},"grandchild_pid":{},"#,
                    r#""tree_ready":true}}"#
                ),
                ready_dir.rsplit(['\\', '/']).next().unwrap_or("unknown"),
                root_pid,
                child_pid,
                grandchild_pid,
            );
            let tmp = format!("{ready_dir}/ready.json.tmp");
            let final_path = format!("{ready_dir}/ready.json");
            if let Ok(mut f) = std::fs::File::create(&tmp) {
                use std::io::Write;
                let _ = f.write_all(ready_json.as_bytes());
                let _ = f.sync_all();
                drop(f);
                let _ = std::fs::rename(&tmp, &final_path);
            }

            // Write sleeping marker to confirm sleep reached.
            // NOTE: stdout is NOT used for readiness or diagnostics.
            // Piped stdout may block under ProcessManager capture load;
            // all control-flow data goes through files.
            let _ = std::fs::write(format!("{ready_dir}/sleeping.txt"), "1");
            thread::sleep(Duration::from_secs(60));
        }
        "ignore_graceful_shutdown" => {
            // Never exits on SIGTERM
            loop {
                thread::sleep(Duration::from_secs(60));
            }
        }
        "ready_signal" => {
            // Print ready marker, then wait for stdin before proceeding
            println!("READY");
            let mut buf = [0u8; 1];
            let _ = io::stdin().read_exact(&mut buf);
            println!("PROCEEDING");
        }
        other => eprintln!("unknown mode: {other}"),
    }
}
