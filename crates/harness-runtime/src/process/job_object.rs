//! Process tree termination.
//! Windows: taskkill /T /F (primary). Job Object abstraction for future upgrade.
//! Unix: process group kill (primary).

use std::io;

#[cfg(windows)]
pub fn kill_process_tree(pid: u32) -> io::Result<()> {
    std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;
    Ok(())
}

#[cfg(not(windows))]
pub fn kill_process_tree(pid: u32) -> io::Result<()> {
    unsafe { libc::kill(-(pid as i32), libc::SIGKILL); }
    Ok(())
}

/// Tokio Child raw_handle() compile probe.
/// Tokio 1.52.3 on Windows provides raw_handle() on Child.
/// Test with: `#[cfg(windows)] { child.raw_handle().is_some() }`
#[allow(dead_code)]
pub fn probe_raw_handle_available() -> bool {
    cfg!(windows)
}
