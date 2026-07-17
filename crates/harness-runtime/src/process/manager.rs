//! ProcessManager — spawn/wait/timeout/cancel with drained output capture.
//!
//! Every spawned child gets:
//! - independent stdout/stderr capture pipelines (see `capture`) so a
//!   flooding child can never deadlock the harness;
//! - a `ProcessTreeGuard` (Windows: Job Object primary, taskkill fallback;
//!   Unix: process group) so cancel/timeout/natural-exit all end the whole
//!   tree with no residual descendants;
//! - a per-execution `ProcessEventRedactor` applied to previews and tracing.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use harness_core::{CoreError, ErrorCode, ErrorSource};
use tokio::sync::RwLock;
use tokio::time::timeout as tokio_timeout;

use super::capture::{spawn_stream_capture, StreamCaptureConfig, StreamCaptureHandle};
use super::job_object::ProcessTreeGuard;
use super::redactor::ProcessEventRedactor;
use super::registry::ProcessRegistry;
use super::types::*;

/// Upper bound on waiting for pipe EOF after the process tree ended.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

pub struct ProcessManager {
    registry: Arc<ProcessRegistry>,
}

impl ProcessManager {
    pub fn new(registry: Arc<ProcessRegistry>) -> Self {
        Self { registry }
    }

    pub async fn spawn(&self, spec: &ProcessSpec) -> Result<ProcessHandle, CoreError> {
        let wants_spool = matches!(spec.stdout_capture, CapturePolicy::Spool { .. })
            || matches!(spec.stderr_capture, CapturePolicy::Spool { .. });
        if wants_spool && spec.spool_dir.is_none() {
            return Err(CoreError::new(
                ErrorCode::ProcessSpawnFailed,
                "CapturePolicy::Spool requires ProcessSpec.spool_dir (harness artifact directory)",
                ErrorSource::System,
            ));
        }

        let redactor = Arc::new(ProcessEventRedactor::with_secrets(
            spec.known_secrets.iter().cloned(),
        ));

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

        // Defense-in-depth: validate env_overrides against profile-allowed set.
        // Any override whose name is sensitive AND not in the allowed set is
        // rejected with a structured error (fail-closed, never silently drop).
        for k in spec.env_overrides.keys() {
            if is_sensitive_env(k) && !spec.allowed_env_var_names.contains(k) {
                return Err(CoreError::new(
                    ErrorCode::ConfigInvalid,
                    format!(
                        "env override '{}' is not in the profile-allowed set for execution {}",
                        k, spec.execution_id
                    ),
                    ErrorSource::Harness,
                ));
            }
        }

        let base_env: HashMap<String, String> = std::env::vars()
            .filter(|(k, _)| !spec.env_removals.contains(k) && is_safe_env(k))
            .collect();
        cmd.env_clear();
        for (k, v) in &base_env {
            cmd.env(k, v);
        }
        // Defense-in-depth re-check on overrides before injection.
        for (k, v) in &spec.env_overrides {
            if !is_sensitive_env(k) || spec.allowed_env_var_names.contains(k) {
                cmd.env(k, v);
            }
        }
        // Environment is logged as names + presence only — never values.
        tracing::debug!(
            execution_id = %spec.execution_id,
            env_override_names = ?ProcessEventRedactor::env_presence(&spec.env_overrides),
            env_removals = ?spec.env_removals,
            allowed_env_names = ?spec.allowed_env_var_names,
            "process_env_prepared"
        );

        let mut child = cmd.spawn().map_err(|e| {
            CoreError::new(
                ErrorCode::ProcessSpawnFailed,
                // Redact known secrets from error text (paths/args may embed them).
                redactor.redact_str(&format!("spawn {}: {e}", spec.executable.display())),
                ErrorSource::System,
            )
        })?;
        let pid = child.id().expect("process must have PID");

        // Tree ownership: Job Object primary on Windows (taskkill fallback).
        let tree_guard = ProcessTreeGuard::attach(&child);
        tracing::debug!(
            execution_id = %spec.execution_id,
            pid,
            job_object = tree_guard.job_object_active(),
            "process_tree_guard_attached"
        );

        // Independent capture pipelines per stream.
        let stdout_capture = child.stdout.take().map(|out| {
            spawn_stream_capture(
                out,
                capture_config(spec, "stdout", &spec.stdout_capture, redactor.clone()),
            )
        });
        let stderr_capture = child.stderr.take().map(|err| {
            spawn_stream_capture(
                err,
                capture_config(spec, "stderr", &spec.stderr_capture, redactor.clone()),
            )
        });

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
        if let StdinMode::OneShot(ref data) = spec.stdin_mode {
            if let Some(mut s) = stdin.take() {
                use tokio::io::AsyncWriteExt;
                let _ = s.write_all(data.as_bytes()).await;
                drop(s);
            }
        }

        let cancel2 = cancel.clone();
        let timeout_dur = spec.timeout;
        let reap_timeout = spec.graceful_shutdown_timeout.max(Duration::from_secs(1));
        let execution_id = spec.execution_id.clone();

        tokio::spawn(async move {
            let start = std::time::Instant::now();

            enum End {
                Natural(std::process::ExitStatus),
                Timeout,
                Cancelled,
                WaitFailed,
            }

            let end = tokio::select! {
                _ = cancel2.cancelled() => {
                    tree_guard.kill_tree();
                    let _ = tokio_timeout(reap_timeout, child.wait()).await;
                    End::Cancelled
                }
                result = tokio_timeout(timeout_dur, child.wait()) => {
                    match result {
                        Ok(Ok(status)) => End::Natural(status),
                        Ok(Err(_e)) => End::WaitFailed,
                        Err(_elapsed) => {
                            tree_guard.kill_tree();
                            let _ = tokio_timeout(reap_timeout, child.wait()).await;
                            End::Timeout
                        }
                    }
                }
            };

            // The execution is over: terminate any residual descendants
            // (orphaned grandchildren survive their parent) so pipes close
            // and nothing outlives the execution. Idempotent after a kill.
            tree_guard.kill_tree();

            // Drain both streams to EOF (bounded).
            let stdout_res = match stdout_capture {
                Some(h) => Some(StreamCaptureHandle::finish(h, DRAIN_TIMEOUT).await),
                None => None,
            };
            let stderr_res = match stderr_capture {
                Some(h) => Some(StreamCaptureHandle::finish(h, DRAIN_TIMEOUT).await),
                None => None,
            };

            let duration_ms = start.elapsed().as_millis() as u64;
            let mut outcome = match end {
                End::Natural(status) => {
                    let t = if status.success() {
                        ProcessTermination::Completed
                    } else {
                        ProcessTermination::NonZeroExit
                    };
                    ProcessOutcome::skeleton(t, status.code(), duration_ms)
                }
                End::Timeout => {
                    ProcessOutcome::skeleton(ProcessTermination::Timeout, None, duration_ms)
                }
                End::Cancelled => {
                    ProcessOutcome::skeleton(ProcessTermination::Cancelled, None, duration_ms)
                }
                End::WaitFailed => {
                    ProcessOutcome::skeleton(ProcessTermination::Lost, None, duration_ms)
                }
            };
            if let Some(r) = stdout_res {
                outcome.stdout_bytes = r.total_bytes;
                outcome.stdout_truncated = r.truncated;
                outcome.stdout_ref = r.spool_ref;
                outcome.stdout_preview = Some(r.preview);
            }
            if let Some(r) = stderr_res {
                outcome.stderr_bytes = r.total_bytes;
                outcome.stderr_truncated = r.truncated;
                outcome.stderr_ref = r.spool_ref;
                outcome.stderr_preview = Some(r.preview);
            }

            tracing::debug!(
                execution_id = %execution_id,
                termination = ?outcome.termination,
                exit_code = ?outcome.exit_code,
                stdout_bytes = outcome.stdout_bytes,
                stderr_bytes = outcome.stderr_bytes,
                "process_completed"
            );

            // Closing the guard (drop) releases the Job Object handle;
            // KILL_ON_JOB_CLOSE reaps anything that raced the terminate.
            drop(tree_guard);

            // ProcessOutcome is produced at most once (guarded write).
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

fn capture_config(
    spec: &ProcessSpec,
    stream_name: &'static str,
    policy: &CapturePolicy,
    redactor: Arc<ProcessEventRedactor>,
) -> StreamCaptureConfig {
    StreamCaptureConfig {
        execution_id: spec.execution_id.clone(),
        stream_name,
        spool_after_bytes: match policy {
            CapturePolicy::Spool { max_memory_bytes } => Some(*max_memory_bytes),
            CapturePolicy::Pipe | CapturePolicy::Discard => None,
        },
        spool_dir: spec.spool_dir.clone(),
        byte_limit: spec.output_byte_limit,
        redactor,
    }
}

fn is_safe_env(key: &str) -> bool {
    !is_sensitive_env(key)
}

/// Returns true if the env var name matches a known credential/secret pattern.
/// Used for defense-in-depth filtering — never inspects values.
fn is_sensitive_env(key: &str) -> bool {
    matches!(
        key.to_uppercase().as_str(),
        "ANTHROPIC_API_KEY"
            | "OPENAI_API_KEY"
            | "DEEPSEEK_API_KEY"
            | "CODEWORKSPACE_TOKEN"
            | "GITHUB_TOKEN"
            | "NPM_TOKEN"
    )
}
