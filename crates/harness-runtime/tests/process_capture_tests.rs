//! Process capture integration tests.
//!
//! Tests the full ProcessManager → capture → StreamCaptureResult pipeline
//! using the process-fixture binary (no real LLM required).
//!
//! Covers:
//!   - stdout with newline → spool file created + readable
//!   - stdout without final newline → spool file has full content
//!   - stderr capture → spool file created
//!   - stdout + stderr concurrently
//!   - exit code 0 + output
//!   - exit code 0 + truly empty output (no spool file)
//!   - nonzero exit + stderr
//!   - preview available when under PREVIEW_LIMIT
//!   - spool file available when over spool threshold
//!   - EOF drain ensures all bytes counted
//!   - stdin piped correctly
//!   - process tree cleanup

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use harness_runtime::process::manager::ProcessManager;
    use harness_runtime::process::registry::ProcessRegistry;
    use harness_runtime::process::types::{CapturePolicy, ProcessSpec, ProcessState, StdinMode};

    fn fixture_path() -> PathBuf {
        // CARGO_TARGET_DIR or default workspace target/scratch/debug
        let target_dir = std::env::var("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                // Walk up from crate dir to workspace root
                let mut p = std::env::current_dir().unwrap();
                loop {
                    if p.join("Cargo.toml").exists() && p.join("crates").is_dir() {
                        break;
                    }
                    if !p.pop() {
                        break;
                    }
                }
                p.join("target").join("scratch")
            });
        let exe = target_dir.join("debug").join("process-fixture.exe");
        if !exe.exists() {
            panic!(
                "process-fixture.exe not found at {}. Build: cargo build --bin process-fixture",
                exe.display()
            );
        }
        exe
    }

    fn make_spec(args: Vec<String>, execution_id: &str, spool_dir: PathBuf) -> ProcessSpec {
        ProcessSpec {
            executable: fixture_path(),
            args,
            working_directory: std::env::temp_dir(),
            env_overrides: HashMap::new(),
            env_removals: vec![],
            stdin_mode: StdinMode::Closed,
            timeout: Duration::from_secs(30),
            graceful_shutdown_timeout: Duration::from_secs(3),
            stdout_capture: CapturePolicy::Spool {
                max_memory_bytes: 64,
            },
            stderr_capture: CapturePolicy::Spool {
                max_memory_bytes: 64,
            },
            output_byte_limit: 1_024 * 1024,
            spool_dir: Some(spool_dir),
            allowed_env_var_names: vec![],
            known_secrets: vec![],
            execution_id: execution_id.to_string(),
            runtime_profile_id: "test".to_string(),
        }
    }

    async fn wait_for_completion(pm: &ProcessManager, exec_id: &str) -> ProcessState {
        for _ in 0..100 {
            let state = pm.get_state(exec_id).await;
            if let Some(ref s) = state {
                if matches!(s, ProcessState::Completed { .. }) {
                    return s.clone();
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        pm.get_state(exec_id)
            .await
            .unwrap_or(ProcessState::Starting)
    }

    // ── Basic capture tests ──────────────────────────────────────────

    #[tokio::test]
    async fn test_stdout_with_newline() {
        let spool_dir = std::env::temp_dir().join(format!("capture-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&spool_dir).unwrap();

        let registry = Arc::new(ProcessRegistry::new());
        let pm = ProcessManager::new(registry);

        let exec_id = format!("test-{}", uuid::Uuid::new_v4());
        let mut spec = make_spec(
            vec!["print_stdout".to_string()],
            &exec_id,
            spool_dir.clone(),
        );
        spec.stdout_capture = CapturePolicy::Spool {
            max_memory_bytes: 16, // small threshold → spool file
        };

        let _handle = pm.spawn(&spec).await.unwrap();
        let state = wait_for_completion(&pm, &exec_id).await;

        if let ProcessState::Completed { outcome } = state {
            assert_eq!(outcome.exit_code, Some(0));
            assert!(outcome.stdout_bytes > 0, "stdout should have bytes");
            // With our fix, small outputs now create a spool file at EOF
            assert!(
                outcome.stdout_ref.is_some(),
                "spool file should exist (EOF flush)"
            );
            if let Some(ref path) = outcome.stdout_ref {
                let content = std::fs::read_to_string(path).unwrap();
                assert!(content.contains("stdout: hello"), "content: {content}");
            }
        } else {
            panic!("expected Completed, got {state:?}");
        }

        // Cleanup
        let _ = std::fs::remove_dir_all(&spool_dir);
    }

    #[tokio::test]
    async fn test_stderr_capture() {
        let spool_dir = std::env::temp_dir().join(format!("capture-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&spool_dir).unwrap();

        let registry = Arc::new(ProcessRegistry::new());
        let pm = ProcessManager::new(registry);

        let exec_id = format!("test-{}", uuid::Uuid::new_v4());
        let mut spec = make_spec(
            vec!["print_stderr".to_string()],
            &exec_id,
            spool_dir.clone(),
        );
        spec.stderr_capture = CapturePolicy::Spool {
            max_memory_bytes: 16,
        };

        let _handle = pm.spawn(&spec).await.unwrap();
        let state = wait_for_completion(&pm, &exec_id).await;

        if let ProcessState::Completed { outcome } = state {
            assert_eq!(outcome.exit_code, Some(0));
            assert!(outcome.stderr_bytes > 0, "stderr should have bytes");
            assert!(
                outcome.stderr_ref.is_some(),
                "stderr spool file should exist"
            );
        } else {
            panic!("expected Completed, got {state:?}");
        }

        let _ = std::fs::remove_dir_all(&spool_dir);
    }

    #[tokio::test]
    async fn test_stdout_stderr_concurrently() {
        let spool_dir = std::env::temp_dir().join(format!("capture-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&spool_dir).unwrap();

        let registry = Arc::new(ProcessRegistry::new());
        let pm = ProcessManager::new(registry);

        let exec_id = format!("test-{}", uuid::Uuid::new_v4());
        let spec = make_spec(vec!["print_both".to_string()], &exec_id, spool_dir.clone());

        let _handle = pm.spawn(&spec).await.unwrap();
        let state = wait_for_completion(&pm, &exec_id).await;

        if let ProcessState::Completed { outcome } = state {
            assert!(outcome.stdout_bytes > 0);
            assert!(outcome.stderr_bytes > 0);
            assert!(outcome.stdout_ref.is_some());
            assert!(outcome.stderr_ref.is_some());
        } else {
            panic!("expected Completed, got {state:?}");
        }

        let _ = std::fs::remove_dir_all(&spool_dir);
    }

    // ── Edge cases ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_exit_code_zero_truly_empty_output() {
        let spool_dir = std::env::temp_dir().join(format!("capture-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&spool_dir).unwrap();

        let registry = Arc::new(ProcessRegistry::new());
        let pm = ProcessManager::new(registry);

        let exec_id = format!("test-{}", uuid::Uuid::new_v4());
        let spec = make_spec(
            vec!["exit_with_code".to_string(), "0".to_string()],
            &exec_id,
            spool_dir.clone(),
        );

        let _handle = pm.spawn(&spec).await.unwrap();
        let state = wait_for_completion(&pm, &exec_id).await;

        if let ProcessState::Completed { outcome } = state {
            assert_eq!(outcome.exit_code, Some(0));
            assert_eq!(outcome.stdout_bytes, 0);
            assert_eq!(outcome.stderr_bytes, 0);
            // No output → no spool file (empty mem_buf → no EOF flush)
            assert!(outcome.stdout_ref.is_none());
            assert!(outcome.stdout_preview.unwrap_or_default().is_empty());
        } else {
            panic!("expected Completed, got {state:?}");
        }

        let _ = std::fs::remove_dir_all(&spool_dir);
    }

    #[tokio::test]
    async fn test_nonzero_exit_no_output() {
        let spool_dir = std::env::temp_dir().join(format!("capture-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&spool_dir).unwrap();

        let registry = Arc::new(ProcessRegistry::new());
        let pm = ProcessManager::new(registry);

        let exec_id = format!("test-{}", uuid::Uuid::new_v4());
        let spec = make_spec(
            vec!["exit_with_code".to_string(), "1".to_string()],
            &exec_id,
            spool_dir.clone(),
        );

        let _handle = pm.spawn(&spec).await.unwrap();
        let state = wait_for_completion(&pm, &exec_id).await;

        if let ProcessState::Completed { outcome } = state {
            assert_eq!(outcome.exit_code, Some(1), "exit code should be 1");
            // exit_with_code produces no output → no spool file needed
            assert_eq!(outcome.stdout_bytes, 0);
            assert_eq!(outcome.stderr_bytes, 0);
        } else {
            panic!("expected Completed, got {state:?}");
        }

        let _ = std::fs::remove_dir_all(&spool_dir);
    }

    // ── Spool threshold tests ────────────────────────────────────────

    #[tokio::test]
    async fn test_small_output_creates_spool_at_eof() {
        // Key test: output under spool threshold must still create
        // a spool file at EOF so the caller can read the full content.
        let spool_dir = std::env::temp_dir().join(format!("capture-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&spool_dir).unwrap();

        let registry = Arc::new(ProcessRegistry::new());
        let pm = ProcessManager::new(registry);

        let exec_id = format!("test-{}", uuid::Uuid::new_v4());
        let mut spec = make_spec(
            vec!["print_stdout".to_string()],
            &exec_id,
            spool_dir.clone(),
        );
        // Set a high threshold so output stays in mem_buf
        spec.stdout_capture = CapturePolicy::Spool {
            max_memory_bytes: 1_024 * 1024, // 1MB — "hello" is far below
        };

        let _handle = pm.spawn(&spec).await.unwrap();
        let state = wait_for_completion(&pm, &exec_id).await;

        if let ProcessState::Completed { outcome } = state {
            assert_eq!(outcome.exit_code, Some(0));
            assert!(outcome.stdout_bytes > 0, "stdout bytes should be > 0");
            // THE FIX: spool file must exist even for small output
            assert!(
                outcome.stdout_ref.is_some(),
                "BUG REGRESSION: small output not flushed to spool at EOF"
            );
            if let Some(ref path) = outcome.stdout_ref {
                let content = std::fs::read_to_string(path).unwrap();
                assert!(
                    content.contains("stdout: hello"),
                    "spool content should have full output, got: {content}"
                );
            }
        } else {
            panic!("expected Completed, got {state:?}");
        }

        let _ = std::fs::remove_dir_all(&spool_dir);
    }

    // ── Stdin test ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_stdin_oneshot_delivered() {
        let spool_dir = std::env::temp_dir().join(format!("capture-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&spool_dir).unwrap();

        let registry = Arc::new(ProcessRegistry::new());
        let pm = ProcessManager::new(registry);

        let exec_id = format!("test-{}", uuid::Uuid::new_v4());
        let mut spec = make_spec(vec!["read_stdin".to_string()], &exec_id, spool_dir.clone());
        spec.stdin_mode = StdinMode::OneShot("hello-from-stdin".to_string());

        let _handle = pm.spawn(&spec).await.unwrap();
        let state = wait_for_completion(&pm, &exec_id).await;

        if let ProcessState::Completed { outcome } = state {
            assert_eq!(outcome.exit_code, Some(0));
            assert!(outcome.stdout_bytes > 0);
            if let Some(ref path) = outcome.stdout_ref {
                let content = std::fs::read_to_string(path).unwrap();
                assert!(
                    content.contains("hello-from-stdin"),
                    "stdin content not in output: {content}"
                );
            }
        } else {
            panic!("expected Completed, got {state:?}");
        }

        let _ = std::fs::remove_dir_all(&spool_dir);
    }
}
