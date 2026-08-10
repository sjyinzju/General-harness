//! I8B TUI — the interactive goal console for `general-harness`.
//!
//! Hard boundary: production code in this module depends ONLY on
//! `harness_core` presentation / IPC contracts plus the Named Pipe
//! transport. It never imports repository, service, or sqlx code, and it
//! never opens the business database — every mutation travels
//! TUI → IPC → Supervisor → production services.
//!
//! The `cfg(test)` integration tests are the exception by design: they spin
//! up a real in-process Supervisor behind a live Named Pipe to prove the
//! full control path end to end.

pub mod action;
pub mod commands;
pub mod gateway;
pub mod input;
pub mod reducer;
pub mod render;
pub mod runner;
pub mod spec;
pub mod state;
pub mod terminal;
pub mod widgets;

#[cfg(test)]
mod bootstrap_tests;

#[cfg(test)]
mod integration_tests;

use std::path::Path;

use crate::ipc_client::SupervisorClient;

/// Options controlling the TUI session.
#[derive(Debug, Clone)]
pub struct TuiOptions {
    /// IPC endpoint (Named Pipe name).
    pub endpoint: String,
    /// Human-readable project label for the header.
    pub project_label: String,
    /// Repo root forwarded when auto-starting the supervisor.
    pub repo_root: Option<String>,
    /// Database path forwarded when auto-starting the supervisor.
    pub db_path: Option<String>,
}

/// Entry point: ensure a supervisor is reachable, then run the shell.
///
/// Supervisor bootstrap reuses the existing production mechanism (detached
/// `supervisor run` child). The TUI never creates a second daemon and never
/// kills the supervisor on exit.
pub async fn run_tui(options: TuiOptions) -> Result<(), TuiError> {
    ensure_supervisor(&options).await?;
    let gateway = gateway::PipeGateway::new(options.endpoint.clone());
    runner::run(gateway, options).await
}

/// Ping the supervisor; if unreachable, spawn it via the existing
/// `supervisor start` mechanics and wait (bounded) for health.
///
/// If the child exits before the pipe comes up, returns immediately with
/// the exit status and a bounded stderr tail so the user sees the real
/// crash reason instead of a generic 15s timeout.
async fn ensure_supervisor(options: &TuiOptions) -> Result<(), TuiError> {
    let client = SupervisorClient::new(&options.endpoint);
    if client.ping().await.unwrap_or(false) {
        return Ok(());
    }

    eprintln!("Supervisor not reachable — starting it (harness supervisor start)...");
    let db_path = options
        .db_path
        .clone()
        .unwrap_or_else(|| "target/data/harness.db".to_string());

    // Compute a worktree root that is outside any git worktree.
    // The default `repo_root/target/tmp` is rejected by WorktreeManager
    // when the repo itself is a git worktree, causing the child to crash
    // during bootstrap before the IPC server starts.
    let worktree_root = safe_worktree_root(options.repo_root.as_deref());

    let mut child = crate::commands::supervisor::cmd_supervisor_start(
        &db_path,
        "default",
        options.repo_root.as_deref(),
        worktree_root.as_deref(),
        None,
        false,
    )
    .await
    .map_err(|e| TuiError::SupervisorBootstrap(format!("{e}")))?;

    // Bounded wait for the pipe to come up: 250ms → 5s backoff, ~15s total.
    let mut delay = std::time::Duration::from_millis(250);
    let mut waited = std::time::Duration::ZERO;
    let cap = std::time::Duration::from_secs(15);
    while waited < cap {
        tokio::time::sleep(delay).await;
        waited += delay;

        // Detect early exit — if the child has already terminated the
        // pipe will never come up.  Surface the crash reason immediately
        // instead of waiting the full 15s.
        match child.try_wait() {
            Ok(Some(status)) => {
                let stderr_tail = read_child_stderr(&mut child, 4096);
                let detail = match (status.code(), stderr_tail) {
                    (Some(code), Some(tail)) => format!(
                        "supervisor exited (code {code}) before becoming healthy; stderr:\n{tail}"
                    ),
                    (Some(code), None) => {
                        format!("supervisor exited (code {code}) before becoming healthy")
                    }
                    (None, Some(tail)) => format!(
                        "supervisor terminated by signal before becoming healthy; stderr:\n{tail}"
                    ),
                    (None, None) => {
                        "supervisor terminated by signal before becoming healthy".to_string()
                    }
                };
                return Err(TuiError::SupervisorBootstrap(detail));
            }
            Ok(None) => {} // still running
            Err(e) => {
                return Err(TuiError::SupervisorBootstrap(format!(
                    "failed to poll supervisor child: {e}"
                )));
            }
        }

        if client.ping().await.unwrap_or(false) {
            return Ok(());
        }
        delay = (delay * 2).min(std::time::Duration::from_secs(5));
    }
    Err(TuiError::SupervisorBootstrap(
        "supervisor did not become healthy within 15s".to_string(),
    ))
}

/// Compute a worktree root that is guaranteed to be outside any git
/// worktree.
///
/// When the repo root is inside a git repository (the common case), the
/// default `repo_root/target/tmp` is rejected by `WorktreeManager`.
/// This function falls back to a platform-appropriate harness data
/// directory outside the repository.
fn safe_worktree_root(repo_root: Option<&str>) -> Option<String> {
    let repo = match repo_root {
        Some(r) => r.to_string(),
        None => std::env::current_dir().ok()?.to_string_lossy().to_string(),
    };
    let repo_path = std::path::Path::new(&repo);

    // Default: repo_root/target/tmp — fine if the repo is NOT a git worktree.
    let default = repo_path.join("target").join("tmp");
    if !path_inside_git_worktree(&default) {
        return Some(default.to_string_lossy().to_string());
    }

    // Fallback: a harness-specific directory under the platform's local
    // app-data location.  This is outside the git worktree by construction.
    let base = if cfg!(windows) {
        std::env::var_os("LOCALAPPDATA")
            .map(std::path::PathBuf::from)
            .or_else(|| std::env::var_os("USERPROFILE").map(std::path::PathBuf::from))
            .unwrap_or_else(|| std::path::PathBuf::from("."))
    } else {
        std::env::var_os("XDG_DATA_HOME")
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(|h| std::path::PathBuf::from(h).join(".local").join("share"))
            })
            .unwrap_or_else(|| std::path::PathBuf::from("."))
    };

    let safe = base.join("harness").join("worktrees");
    Some(safe.to_string_lossy().to_string())
}

/// Check whether a path is inside a git worktree by walking up the
/// ancestor chain looking for a `.git` entry, stopping at the user's
/// home directory.  Mirrors the logic in
/// `harness_runtime::artifact::find_git_ancestor`.
fn path_inside_git_worktree(path: &std::path::Path) -> bool {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        match std::env::current_dir() {
            Ok(cwd) => cwd.join(path),
            Err(_) => return false,
        }
    };
    let home = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(std::path::PathBuf::from);
    for ancestor in absolute.ancestors() {
        if home.as_deref() == Some(ancestor) {
            break;
        }
        if ancestor.join(".git").exists() {
            return true;
        }
    }
    false
}

/// Read up to `max_bytes` from the child's piped stderr, returning the
/// tail as a UTF-8-lossy string.  Called only after the child has exited,
/// so the read is non-blocking (data is buffered by the OS).
fn read_child_stderr(child: &mut std::process::Child, max_bytes: usize) -> Option<String> {
    use std::io::Read;
    let stderr = child.stderr.as_mut()?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        match stderr.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.len() > max_bytes * 2 {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    if buf.is_empty() {
        return None;
    }
    if buf.len() > max_bytes {
        let start = buf.len() - max_bytes;
        buf = buf[start..].to_vec();
    }
    Some(String::from_utf8_lossy(&buf).to_string())
}

/// Resolve a compact project label from the repo root path.
pub fn project_label_from_path(repo_root: &Path) -> String {
    repo_root
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| repo_root.to_string_lossy().to_string())
}

/// Errors surfaced by the TUI entry path (rendered as plain text *before*
/// the terminal guard activates, so the user terminal is never corrupted).
#[derive(Debug)]
pub enum TuiError {
    SupervisorBootstrap(String),
    Terminal(String),
    Run(String),
}

impl std::fmt::Display for TuiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TuiError::SupervisorBootstrap(msg) => {
                write!(f, "supervisor bootstrap failed: {msg}")
            }
            TuiError::Terminal(msg) => write!(f, "terminal error: {msg}"),
            TuiError::Run(msg) => write!(f, "tui error: {msg}"),
        }
    }
}

impl std::error::Error for TuiError {}
