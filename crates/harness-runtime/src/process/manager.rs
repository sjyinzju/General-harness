use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use harness_core::{CoreError, ErrorCode, ErrorSource};
use tokio::sync::RwLock;
use tokio::time::timeout as tokio_timeout;

use super::job_object::kill_process_tree;
use super::registry::ProcessRegistry;
use super::types::*;

pub struct ProcessManager {
    registry: Arc<ProcessRegistry>,
}

impl ProcessManager {
    pub fn new(registry: Arc<ProcessRegistry>) -> Self {
        Self { registry }
    }

    pub async fn spawn(&self, spec: &ProcessSpec) -> Result<ProcessHandle, CoreError> {
        let cancel = tokio_util::sync::CancellationToken::new();
        let mut cmd = tokio::process::Command::new(&spec.executable);
        cmd.args(&spec.args)
            .current_dir(&spec.working_directory)
            .stdin(match &spec.stdin_mode {
                StdinMode::Closed => Stdio::null(),
                StdinMode::Pipe | StdinMode::OneShot(_) => Stdio::piped(),
            })
            .stdout(match spec.stdout_capture {
                CapturePolicy::Pipe | CapturePolicy::Spool { .. } => Stdio::piped(),
                CapturePolicy::Discard => Stdio::null(),
            })
            .stderr(match spec.stderr_capture {
                CapturePolicy::Pipe | CapturePolicy::Spool { .. } => Stdio::piped(),
                CapturePolicy::Discard => Stdio::null(),
            });

        let base_env: HashMap<String, String> = std::env::vars()
            .filter(|(k, _)| !spec.env_removals.contains(k) && is_safe_env(k))
            .collect();
        cmd.env_clear();
        for (k, v) in &base_env {
            cmd.env(k, v);
        }
        for (k, v) in &spec.env_overrides {
            cmd.env(k, v);
        }

        let mut child = cmd.spawn().map_err(|e| {
            CoreError::new(
                ErrorCode::ProcessSpawnFailed,
                format!("spawn {}: {e}", spec.executable.display()),
                ErrorSource::System,
            )
        })?;
        let pid = child.id().expect("process must have PID");

        let state = Arc::new(RwLock::new(ProcessState::Running));
        let handle = ProcessHandle {
            execution_id: spec.execution_id.clone(),
            pid,
            start_time: chrono::Utc::now(),
            state: state.clone(),
        };
        self.registry
            .register_with_state(
                spec.execution_id.clone(),
                pid,
                cancel.clone(),
                state.clone(),
            )
            .await;

        let mut stdin = child.stdin.take();
        let cancel2 = cancel.clone();
        let timeout_dur = spec.timeout;
        let graceful = spec.graceful_shutdown_timeout;

        if let StdinMode::OneShot(ref data) = spec.stdin_mode {
            if let Some(mut s) = stdin.take() {
                use tokio::io::AsyncWriteExt;
                let _ = s.write_all(data.as_bytes()).await;
                drop(s);
            }
        }

        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let outcome = tokio::select! {
                _ = cancel2.cancelled() => {
                    let _ = kill_process_tree(pid);
                    ProcessOutcome { termination: ProcessTermination::Cancelled, exit_code: None,
                        stdout_bytes: 0, stderr_bytes: 0, duration_ms: start.elapsed().as_millis() as u64,
                        stdout_ref: None, stderr_ref: None }
                }
                result = tokio_timeout(timeout_dur, child.wait()) => {
                    match result {
                        Ok(Ok(status)) => {
                            let t = if status.success() { ProcessTermination::Completed } else { ProcessTermination::NonZeroExit };
                            ProcessOutcome { termination: t, exit_code: status.code(),
                                stdout_bytes: 0, stderr_bytes: 0, duration_ms: start.elapsed().as_millis() as u64,
                                stdout_ref: None, stderr_ref: None }
                        }
                        Ok(Err(_e)) => ProcessOutcome { termination: ProcessTermination::SpawnFailed, exit_code: None,
                            stdout_bytes: 0, stderr_bytes: 0, duration_ms: start.elapsed().as_millis() as u64,
                            stdout_ref: None, stderr_ref: None },
                        Err(_elapsed) => {
                            let _ = kill_process_tree(pid);
                            ProcessOutcome { termination: ProcessTermination::Timeout, exit_code: None,
                                stdout_bytes: 0, stderr_bytes: 0, duration_ms: timeout_dur.as_millis() as u64,
                                stdout_ref: None, stderr_ref: None }
                        }
                    }
                }
            };
            let _ = (graceful,);
            let mut s = state.write().await;
            if matches!(&*s, ProcessState::Running) {
                *s = ProcessState::Completed { outcome };
            }
        });

        Ok(handle)
    }

    pub async fn cancel(&self, execution_id: &str) -> Result<(), CoreError> {
        self.registry.cancel(execution_id).await
    }

    pub async fn get_state(&self, execution_id: &str) -> Option<ProcessState> {
        self.registry.get_state(execution_id).await
    }
}

fn is_safe_env(key: &str) -> bool {
    !matches!(
        key.to_uppercase().as_str(),
        "ANTHROPIC_API_KEY"
            | "OPENAI_API_KEY"
            | "DEEPSEEK_API_KEY"
            | "CODEWORKSPACE_TOKEN"
            | "GITHUB_TOKEN"
            | "NPM_TOKEN"
    )
}
