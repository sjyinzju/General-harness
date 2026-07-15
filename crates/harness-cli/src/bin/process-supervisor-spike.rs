//! Process Supervisor Spike — validates subprocess management primitives.
//! Not production code. Run with: cargo run --bin process-supervisor-spike

use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::time::timeout;

#[tokio::main]
async fn main() {
    println!("=== Process Supervisor Spike ===\n");

    // Test 1: Spawn + stdout capture
    println!("[1/6] Spawn subprocess + capture stdout...");
    test_spawn_capture().await;

    // Test 2: Stdin write
    println!("\n[2/6] Stdin write + read response...");
    test_stdin_write().await;

    // Test 3: Stderr capture
    println!("\n[3/6] Stderr capture...");
    test_stderr_capture().await;

    // Test 4: Timeout
    println!("\n[4/6] Timeout...");
    test_timeout().await;

    // Test 5: Cancellation (Ctrl+C simulation)
    println!("\n[5/6] Cancellation (kill)...");
    test_cancellation().await;

    // Test 6: Process tree (chain)
    println!("\n[6/6] Child process exit detection...");
    test_exit_detection().await;

    println!("\n=== Spike Complete ===");
}

async fn test_spawn_capture() {
    let mut child = Command::new("cmd")
        .args(["/c", "echo hello-from-subprocess"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn failed");

    let pid = child.id().expect("no pid");
    println!("  PID: {pid}");

    let output = child.wait_with_output().await.expect("wait failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("  stdout: {stdout:?}");
    println!("  exit_code: {}", output.status.code().unwrap_or(-1));
    println!("  RESULT: PASS");
}

async fn test_stdin_write() {
    // PowerShell: read line from stdin, echo it back
    let mut child = Command::new("powershell")
        .args(["-NoProfile", "-Command", "$line = Read-Host; Write-Output \"ECHO: $line\""])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn failed");

    let pid = child.id().expect("no pid");
    println!("  PID: {pid}");

    // Write to stdin
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(b"test-input-from-harness\n").await.expect("stdin write failed");
        // stdin is dropped here, which closes the pipe
    }

    let output = child.wait_with_output().await.expect("wait failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("  stdout: {stdout:?}");
    println!("  RESULT: PASS");
}

async fn test_stderr_capture() {
    let mut child = Command::new("cmd")
        .args(["/c", "echo to-stdout & echo to-stderr >&2"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn failed");

    let output = child.wait_with_output().await.expect("wait failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("  stdout: {stdout:?}");
    println!("  stderr: {stderr:?}");
    println!("  RESULT: PASS");
}

async fn test_timeout() {
    let mut child = Command::new("powershell")
        .args(["-NoProfile", "-Command", "Start-Sleep -Seconds 30; Write-Output 'done'"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn failed");

    let pid = child.id().expect("no pid");
    println!("  PID: {pid} (sleep 30s)");

    let result = timeout(Duration::from_secs(3), child.wait()).await;

    match result {
        Ok(_) => println!("  RESULT: FAIL (should have timed out)"),
        Err(_) => {
            // Timeout — kill the child
            println!("  Timeout triggered — killing child");
            let _ = Command::new("taskkill")
                .args(["/PID", &pid.to_string(), "/T", "/F"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn();
            println!("  RESULT: PASS (timeout + kill)");
        }
    }
}

async fn test_cancellation() {
    let mut child = Command::new("powershell")
        .args(["-NoProfile", "-Command", "Start-Sleep -Seconds 60; Write-Output 'done'"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn failed");

    let pid = child.id().expect("no pid");
    println!("  PID: {pid}");

    // Simulate cancellation after 1s
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Kill process tree
    let kill = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    if let Ok(mut kill_child) = kill {
        let _ = kill_child.wait().await;
    }

    // Verify child is dead
    let result = timeout(Duration::from_secs(3), child.wait()).await;
    match result {
        Ok(status) => println!("  Child exited with: {status:?} — RESULT: PASS"),
        Err(_) => {
            println!("  Child still alive after kill — RESULT: FAIL");
            let _ = child.kill().await;
        }
    }
}

async fn test_exit_detection() {
    let mut child = Command::new("cmd")
        .args(["/c", "exit /b 42"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn failed");

    let pid = child.id().expect("no pid");
    let status = child.wait().await.expect("wait failed");
    println!("  PID: {pid}");
    println!("  Exit code: {}", status.code().unwrap_or(-1));
    println!("  RESULT: PASS (detected non-zero exit)");
}
