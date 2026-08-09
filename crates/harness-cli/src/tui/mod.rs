//! I8B TUI — the interactive goal console for `general-harness`.
//!
//! Hard boundary: this module depends ONLY on `harness_core` presentation /
//! IPC contracts plus the Named Pipe transport. It never imports repository,
//! service, or sqlx code, and it never opens the business database — every
//! mutation travels TUI → IPC → Supervisor → production services.

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
    crate::commands::supervisor::cmd_supervisor_start(
        &db_path,
        "default",
        options.repo_root.as_deref(),
        None,
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
        if client.ping().await.unwrap_or(false) {
            return Ok(());
        }
        delay = (delay * 2).min(std::time::Duration::from_secs(5));
    }
    Err(TuiError::SupervisorBootstrap(
        "supervisor did not become healthy within 15s".to_string(),
    ))
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
