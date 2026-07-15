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
        "print_both" => { println!("stdout"); eprintln!("stderr"); }
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
                println!("{}={}", key, env::var(key).unwrap_or_else(|_| "<unset>".into()));
            }
        }
        "flood_stdout" => {
            let count: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
            for i in 0..count { println!("stdout line {i}"); }
        }
        "flood_stderr" => {
            let count: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
            for i in 0..count { eprintln!("stderr line {i}"); }
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
            println!("child_pid={}", child_pid.unwrap().id());
        }
        "spawn_grandchild" => {
            let exe = env::current_exe().unwrap();
            let child = process::Command::new(&exe).arg("spawn_child").spawn().unwrap();
            let _ = child.wait_with_output();
        }
        "ignore_graceful_shutdown" => {
            // Never exits on SIGTERM
            loop { thread::sleep(Duration::from_secs(60)); }
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
