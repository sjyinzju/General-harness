//! GitRunner — every git invocation goes through ProcessManager.
//!
//! Guarantees:
//! - executable + args, never a shell;
//! - explicit working directory and timeout;
//! - exit code, stdout, and stderr captured (spooled to a scratch dir, read
//!   back, cleaned up);
//! - success judged ONLY by exit code — never by localized stderr text;
//! - structured `CoreError` on spawn/timeout failures;
//! - user global git config is never modified (only read).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use harness_core::{CoreError, ErrorCode, ErrorSource};

use crate::process::{
    CapturePolicy, ProcessManager, ProcessRegistry, ProcessSpec, ProcessState, ProcessTermination,
    StdinMode,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const OUTPUT_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct GitOutput {
    pub exit_code: Option<i32>,
    pub termination: ProcessTermination,
    pub stdout: String,
    pub stderr: String,
}

impl GitOutput {
    pub fn success(&self) -> bool {
        self.termination == ProcessTermination::Completed && self.exit_code == Some(0)
    }

    /// Structured error for a failed invocation. stderr is attached as a
    /// bounded diagnostic only — success/failure is decided by exit code.
    pub fn into_error(self, context: &str) -> CoreError {
        let stderr_snippet: String = self.stderr.chars().take(500).collect();
        CoreError::new(
            ErrorCode::WorkspaceError,
            format!(
                "git {context} failed: termination={:?} exit={:?}",
                self.termination, self.exit_code
            ),
            ErrorSource::System,
        )
        .with_diagnostic(stderr_snippet)
    }
}

pub struct GitRunner {
    manager: ProcessManager,
    scratch_root: PathBuf,
    /// Extra environment (name → value); tests use this to isolate from the
    /// user's global git config without ever modifying it.
    env_overrides: HashMap<String, String>,
    timeout: Duration,
}

impl GitRunner {
    pub fn new(scratch_root: PathBuf) -> Result<Self, CoreError> {
        std::fs::create_dir_all(&scratch_root).map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("create git scratch root {}: {e}", scratch_root.display()),
                ErrorSource::System,
            )
        })?;
        Ok(Self {
            manager: ProcessManager::new(Arc::new(ProcessRegistry::new())),
            scratch_root,
            env_overrides: HashMap::new(),
            timeout: DEFAULT_TIMEOUT,
        })
    }

    /// Add environment overrides for every git call (e.g. test isolation via
    /// `GIT_CONFIG_GLOBAL`/`GIT_CONFIG_NOSYSTEM`). Never mutates user config.
    pub fn with_env(mut self, env: HashMap<String, String>) -> Self {
        self.env_overrides = env;
        self
    }

    /// Run `git <args>` in `cwd`. Non-zero exit is NOT an `Err` — callers
    /// inspect `GitOutput::success()`; `Err` means spawn/timeout/IO failure.
    pub async fn run(&self, cwd: &Path, args: &[&str]) -> Result<GitOutput, CoreError> {
        let call_id = format!("git-{}", uuid::Uuid::new_v4());
        let spool_dir = self.scratch_root.join(&call_id);
        std::fs::create_dir_all(&spool_dir).map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("create git spool dir: {e}"),
                ErrorSource::System,
            )
        })?;

        let spec = ProcessSpec {
            executable: PathBuf::from("git"),
            args: args.iter().map(|s| (*s).to_string()).collect(),
            working_directory: cwd.to_path_buf(),
            env_overrides: self.env_overrides.clone(),
            env_removals: vec![],
            stdin_mode: StdinMode::Closed,
            timeout: self.timeout,
            graceful_shutdown_timeout: Duration::from_secs(2),
            stdout_capture: CapturePolicy::Spool {
                max_memory_bytes: 0,
            },
            stderr_capture: CapturePolicy::Spool {
                max_memory_bytes: 0,
            },
            output_byte_limit: OUTPUT_LIMIT,
            spool_dir: Some(spool_dir.clone()),
            known_secrets: vec![],
            execution_id: call_id.clone(),
            runtime_profile_id: "git-runner".into(),
        };

        let spawn_result = self.manager.spawn(&spec).await;
        let outcome = match spawn_result {
            Err(e) => {
                let _ = std::fs::remove_dir_all(&spool_dir);
                // Spawn failure — e.g. git executable not found on PATH.
                return Err(CoreError::new(
                    ErrorCode::ProcessSpawnFailed,
                    format!("git executable unavailable: {}", e.message),
                    ErrorSource::System,
                ));
            }
            Ok(_handle) => loop {
                match self.manager.get_state(&call_id).await {
                    Some(ProcessState::Completed { outcome }) => break outcome,
                    Some(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                    None => {
                        let _ = std::fs::remove_dir_all(&spool_dir);
                        return Err(CoreError::new(
                            ErrorCode::PersistenceError,
                            "git process vanished from registry",
                            ErrorSource::System,
                        ));
                    }
                }
            },
        };

        let read_spool = |r: &Option<String>| -> String {
            r.as_deref()
                .and_then(|p| std::fs::read(p).ok())
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default()
        };
        let stdout = read_spool(&outcome.stdout_ref);
        let stderr = read_spool(&outcome.stderr_ref);
        let _ = std::fs::remove_dir_all(&spool_dir);

        if outcome.termination == ProcessTermination::Timeout {
            return Err(CoreError::new(
                ErrorCode::ProcessTimeout {
                    duration_ms: self.timeout.as_millis() as u64,
                },
                format!("git {} timed out after {:?}", args.join(" "), self.timeout),
                ErrorSource::System,
            ));
        }

        Ok(GitOutput {
            exit_code: outcome.exit_code,
            termination: outcome.termination,
            stdout,
            stderr,
        })
    }

    /// Run and require exit code 0; returns trimmed stdout.
    pub async fn run_ok(&self, cwd: &Path, args: &[&str]) -> Result<String, CoreError> {
        let out = self.run(cwd, args).await?;
        if !out.success() {
            return Err(out.into_error(&args.join(" ")));
        }
        Ok(out.stdout.trim_end_matches(['\n', '\r']).to_string())
    }
}
