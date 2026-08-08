//! Diagnostic: verify ProcessManager captures output from .cmd wrappers.
//! This tests the full CREATE_SUSPENDED + .cmd file path.
//! Does NOT make real LLM calls — uses --version only.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use harness_runtime::process::manager::ProcessManager;
    use harness_runtime::process::registry::ProcessRegistry;
    use harness_runtime::process::types::{CapturePolicy, ProcessSpec, ProcessState, StdinMode};

    fn claude_cmd() -> String {
        std::env::var("CLAUDE_CMD_PATH")
            .unwrap_or_else(|_| r"C:\Users\shiju\AppData\Roaming\npm\claude.cmd".to_string())
    }

    #[tokio::test]
    async fn test_claude_version_through_process_manager() {
        let spool_dir = std::env::temp_dir().join(format!("claude-diag-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&spool_dir).unwrap();

        let registry = Arc::new(ProcessRegistry::new());
        let pm = ProcessManager::new(registry);

        let exec_id = format!("claude-diag-{}", uuid::Uuid::new_v4());
        let spec = ProcessSpec {
            executable: claude_cmd().into(),
            args: vec!["--version".to_string()],
            working_directory: std::env::temp_dir(),
            env_overrides: HashMap::new(),
            env_removals: vec![],
            stdin_mode: StdinMode::Closed,
            timeout: Duration::from_secs(30),
            graceful_shutdown_timeout: Duration::from_secs(3),
            stdout_capture: CapturePolicy::Spool {
                max_memory_bytes: 64, // small → spool threshold breached early
            },
            stderr_capture: CapturePolicy::Spool {
                max_memory_bytes: 64,
            },
            output_byte_limit: 4096,
            spool_dir: Some(spool_dir.clone()),
            allowed_env_var_names: vec![],
            known_secrets: vec![],
            execution_id: exec_id.clone(),
            runtime_profile_id: "diag".to_string(),
        };

        let _handle = pm.spawn(&spec).await.expect("spawn should succeed");

        // Wait for completion
        let state = loop {
            let state = pm.get_state(&exec_id).await;
            match state {
                Some(ProcessState::Completed { .. }) => break state.unwrap(),
                Some(ProcessState::Running) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                _ => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };

        if let ProcessState::Completed { outcome } = state {
            eprintln!("=== CLAUDE CMD DIAGNOSTIC ===");
            eprintln!("exit_code: {:?}", outcome.exit_code);
            eprintln!("stdout_bytes: {}", outcome.stdout_bytes);
            eprintln!("stderr_bytes: {}", outcome.stderr_bytes);
            eprintln!("stdout_ref: {:?}", outcome.stdout_ref);
            eprintln!("stderr_ref: {:?}", outcome.stderr_ref);
            eprintln!(
                "stdout_preview: {:?}",
                outcome.stdout_preview.as_deref().unwrap_or("(none)")
            );
            eprintln!(
                "stderr_preview: {:?}",
                outcome.stderr_preview.as_deref().unwrap_or("(none)")
            );
            eprintln!("duration_ms: {}", outcome.duration_ms);

            // Read spool file if available
            if let Some(ref path) = outcome.stdout_ref {
                match std::fs::read_to_string(path) {
                    Ok(content) => {
                        eprintln!("spool content ({} bytes): {content}", content.len());
                    }
                    Err(e) => {
                        eprintln!("spool read error: {e}");
                    }
                }
            }

            // The key assertion
            assert_eq!(outcome.exit_code, Some(0), "claude --version should exit 0");
            assert!(
                outcome.stdout_bytes > 0,
                "CRITICAL: 0 bytes captured from claude.cmd via ProcessManager. \
                 This confirms the capture bug is still present for .cmd wrappers \
                 even after the mem_buf EOF flush fix."
            );
            // With the fix, we should have a spool file for any non-empty output
            assert!(
                outcome.stdout_ref.is_some(),
                "spool file should exist for non-empty output"
            );
        } else {
            panic!("expected Completed state");
        }

        let _ = std::fs::remove_dir_all(&spool_dir);
    }
}
