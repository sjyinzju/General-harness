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
            // Print to stdout for backward compat with process_capture tests.
            println!("child_pid={pid}");
            // Write grandchild PID to grandchild.txt in the readiness dir.
            // Uses READY_DIR if set, otherwise current directory (set by
            // ProcessManager as the working_directory).
            // Best-effort: write grandchild PID to grandchild.txt for
            // the deterministic readiness protocol. Ignore errors for
            // backward compat with spawn_grandchild mode.
            if let Ok(rd) = env::var("READY_DIR") {
                let _ = std::fs::write(format!("{rd}/grandchild.txt"), pid.to_string());
            } else if let Ok(cwd) = env::current_dir() {
                let _ = std::fs::write(cwd.join("grandchild.txt"), pid.to_string());
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

            // ── I4.5 structural fix ──────────────────────────────
            // No more 50ms sleep! The ProcessManager now creates this
            // process with CREATE_SUSPENDED and resumes it only AFTER
            // Job Object assignment is complete. Children created here
            // are born directly into the Job — no escape window.
            //
            // Previously: thread::sleep(Duration::from_millis(50));
            // This was a race mitigation, not a structural fix.

            let exe = env::current_exe().unwrap();
            let ready_dir = env::var("READY_DIR").unwrap_or_else(|_| {
                env::current_dir()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|_| env::temp_dir().to_string_lossy().to_string())
            });
            let root_pid = process::id();

            // Spawn child. Child spawns grandchild (sleep 10), writes
            // grandchild PID to READY_DIR/grandchild.txt, prints
            // "child_pid=<grandchild>" to inherited stdout, then exits.
            // No delay before spawning — the Job Object is already assigned.
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

            // ── I4.5 TCP readiness channel ──────────────────────
            // Primary control protocol: connect to the test's TCP
            // listener, send structured TreeReady JSON, disconnect.
            // The test blocks on accept() — no polling, no race.
            let run_id = env::var("READY_RUN_ID").unwrap_or_else(|_| {
                ready_dir
                    .rsplit(['\\', '/'])
                    .next()
                    .unwrap_or("unknown")
                    .to_string()
            });
            let ready_json = format!(
                concat!(
                    r#"{{"run_id":"{}","root_pid":{},"#,
                    r#""child_pid":{},"grandchild_pid":{},"#,
                    r#""tree_ready":true}}"#
                ),
                run_id, root_pid, child_pid, grandchild_pid,
            );

            // TCP readiness (primary): send to test's listener.
            if let Ok(port_str) = env::var("READY_TCP_PORT") {
                if let Ok(port) = port_str.parse::<u16>() {
                    use std::io::Write;
                    use std::net::TcpStream;
                    let addr = format!("127.0.0.1:{port}");
                    if let Ok(mut stream) = TcpStream::connect(&addr) {
                        let _ = stream.write_all(ready_json.as_bytes());
                        let _ = stream.flush();
                        // Don't close immediately — let the test read.
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
            }

            // Diagnostic backup: atomic file-based ready.json.
            // NOT used as primary readiness signal — only for post-mortem
            // diagnostics. The test uses TCP, not file polling.
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
