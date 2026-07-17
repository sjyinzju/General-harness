//! I4-A Closure: Environment boundary tests.
//! Verifies ProcessManager defense-in-depth env filtering.

use harness_runtime::process::manager::ProcessManager;
use harness_runtime::process::registry::ProcessRegistry;
use harness_runtime::process::types::{CapturePolicy, ProcessSpec, StdinMode};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!("harness-env-test-{}", uuid::Uuid::new_v4()))
}

#[tokio::test]
async fn test_unauthorized_sensitive_override_rejected() {
    let mgr = ProcessManager::new(Arc::new(ProcessRegistry::new()));
    let tmp = temp_dir();
    std::fs::create_dir_all(&tmp).unwrap();

    let spec = ProcessSpec {
        executable: if cfg!(windows) {
            PathBuf::from("cmd.exe")
        } else {
            PathBuf::from("/bin/echo")
        },
        args: if cfg!(windows) {
            vec!["/c".into(), "exit 0".into()]
        } else {
            vec!["test".into()]
        },
        working_directory: tmp.clone(),
        env_overrides: {
            let mut m = HashMap::new();
            m.insert("ANTHROPIC_API_KEY".to_string(), "sk-test".to_string());
            m
        },
        env_removals: vec![],
        stdin_mode: StdinMode::Closed,
        timeout: Duration::from_secs(5),
        graceful_shutdown_timeout: Duration::from_secs(1),
        stdout_capture: CapturePolicy::Discard,
        stderr_capture: CapturePolicy::Discard,
        output_byte_limit: 4096,
        spool_dir: None,
        allowed_env_var_names: vec![], // Not in allowed set
        known_secrets: vec![],
        execution_id: "test-env-reject".into(),
        runtime_profile_id: "test".into(),
    };

    let result = mgr.spawn(&spec).await;
    assert!(
        result.is_err(),
        "Unauthorized sensitive override must be rejected"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("ANTHROPIC_API_KEY"),
        "Error must name the rejected var"
    );
}

#[tokio::test]
async fn test_authorized_sensitive_override_passed() {
    let mgr = ProcessManager::new(Arc::new(ProcessRegistry::new()));
    let tmp = temp_dir();
    std::fs::create_dir_all(&tmp).unwrap();

    let spec = ProcessSpec {
        executable: if cfg!(windows) {
            PathBuf::from("cmd.exe")
        } else {
            PathBuf::from("/bin/echo")
        },
        args: if cfg!(windows) {
            vec!["/c".into(), "exit 0".into()]
        } else {
            vec!["test".into()]
        },
        working_directory: tmp,
        env_overrides: {
            let mut m = HashMap::new();
            m.insert("ANTHROPIC_API_KEY".to_string(), "sk-test".to_string());
            m
        },
        env_removals: vec![],
        stdin_mode: StdinMode::Closed,
        timeout: Duration::from_secs(5),
        graceful_shutdown_timeout: Duration::from_secs(1),
        stdout_capture: CapturePolicy::Discard,
        stderr_capture: CapturePolicy::Discard,
        output_byte_limit: 4096,
        spool_dir: None,
        allowed_env_var_names: vec!["ANTHROPIC_API_KEY".to_string()], // Explicitly allowed
        known_secrets: vec!["sk-test".to_string()],                   // Register for redaction
        execution_id: "test-env-allow".into(),
        runtime_profile_id: "test".into(),
    };

    let result = mgr.spawn(&spec).await;
    assert!(
        result.is_ok(),
        "Authorized override should pass: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_non_sensitive_explicit_var_passed() {
    let mgr = ProcessManager::new(Arc::new(ProcessRegistry::new()));
    let tmp = temp_dir();
    std::fs::create_dir_all(&tmp).unwrap();

    let spec = ProcessSpec {
        executable: if cfg!(windows) {
            PathBuf::from("cmd.exe")
        } else {
            PathBuf::from("/bin/echo")
        },
        args: if cfg!(windows) {
            vec!["/c".into(), "exit 0".into()]
        } else {
            vec!["test".into()]
        },
        working_directory: tmp,
        env_overrides: {
            let mut m = HashMap::new();
            m.insert("MY_APP_CONFIG".to_string(), "some-value".to_string());
            m
        },
        env_removals: vec![],
        stdin_mode: StdinMode::Closed,
        timeout: Duration::from_secs(5),
        graceful_shutdown_timeout: Duration::from_secs(1),
        stdout_capture: CapturePolicy::Discard,
        stderr_capture: CapturePolicy::Discard,
        output_byte_limit: 4096,
        spool_dir: None,
        allowed_env_var_names: vec![], // Not sensitive, so empty allowed set is OK
        known_secrets: vec![],
        execution_id: "test-env-nonsensitive".into(),
        runtime_profile_id: "test".into(),
    };

    let result = mgr.spawn(&spec).await;
    assert!(
        result.is_ok(),
        "Non-sensitive override should pass: {:?}",
        result.err()
    );
}
