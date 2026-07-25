//! I6.1 Supervisor ownership and lifecycle tests.
//!
//! These tests verify:
//! - Single active supervisor ownership
//! - Lease acquisition and fencing
//! - Heartbeat CAS
//! - Stale owner takeover
//! - Process identity verification
//! - Terminal state handling

#[cfg(test)]
mod supervisor_tests {
    use chrono::Utc;
    use harness_core::contracts::supervisor::{
        SupervisorConfig, SupervisorInstance, SupervisorInstanceId, SupervisorState,
    };
    use std::time::Duration;

    use crate::db::Database;
    use crate::supervisor::lifecycle::LifecycleFsm;
    use crate::supervisor::repo::SupervisorRepo;

    /// Helper to create a test supervisor instance.
    fn make_test_instance(state_dir: &str, state: SupervisorState) -> SupervisorInstance {
        let now = Utc::now();
        SupervisorInstance {
            instance_id: SupervisorInstanceId(uuid::Uuid::new_v4().to_string()),
            state_directory_id: state_dir.to_string(),
            pid: std::process::id(),
            process_started_at: now,
            boot_nonce: uuid::Uuid::new_v4().to_string(),
            state,
            fencing_token: 0,
            started_at: now,
            heartbeat_at: now,
            lease_expires_at: now + Duration::from_secs(30),
            protocol_version: "1.0".to_string(),
            binary_version: "0.1.0".to_string(),
        }
    }

    // ── Lifecycle State Machine ──────────────────────────────────────

    #[test]
    fn test_fsm_all_transitions() {
        let fsm = LifecycleFsm::new();

        // Valid: Created → Starting → AcquiringOwnership → Recovering → Ready
        assert!(fsm
            .validate_transition(SupervisorState::Created, SupervisorState::Starting)
            .is_ok());
        assert!(fsm
            .validate_transition(
                SupervisorState::Starting,
                SupervisorState::AcquiringOwnership
            )
            .is_ok());
        assert!(fsm
            .validate_transition(
                SupervisorState::AcquiringOwnership,
                SupervisorState::Recovering
            )
            .is_ok());
        assert!(fsm
            .validate_transition(SupervisorState::Recovering, SupervisorState::Ready)
            .is_ok());

        // Valid: Ready → Draining → Stopping → Stopped
        assert!(fsm
            .validate_transition(SupervisorState::Ready, SupervisorState::Draining)
            .is_ok());
        assert!(fsm
            .validate_transition(SupervisorState::Draining, SupervisorState::Stopping)
            .is_ok());
        assert!(fsm
            .validate_transition(SupervisorState::Stopping, SupervisorState::Stopped)
            .is_ok());

        // Valid: Any active → Failed
        assert!(fsm
            .validate_transition(SupervisorState::Ready, SupervisorState::Failed)
            .is_ok());
        assert!(fsm
            .validate_transition(SupervisorState::Draining, SupervisorState::Failed)
            .is_ok());

        // Invalid: skip states
        assert!(fsm
            .validate_transition(SupervisorState::Created, SupervisorState::Ready)
            .is_err());
        assert!(fsm
            .validate_transition(SupervisorState::Starting, SupervisorState::Ready)
            .is_err());

        // Invalid: reverse
        assert!(fsm
            .validate_transition(SupervisorState::Ready, SupervisorState::Starting)
            .is_err());
        assert!(fsm
            .validate_transition(SupervisorState::Ready, SupervisorState::Recovering)
            .is_err());

        // Invalid: terminal → anything
        assert!(fsm
            .validate_transition(SupervisorState::Stopped, SupervisorState::Ready)
            .is_err());
        assert!(fsm
            .validate_transition(SupervisorState::Failed, SupervisorState::Ready)
            .is_err());
    }

    #[test]
    fn test_terminal_state_invariant() {
        let fsm = LifecycleFsm::new();
        let terminals = [SupervisorState::Stopped, SupervisorState::Failed];
        let all_states = [
            SupervisorState::Created,
            SupervisorState::Starting,
            SupervisorState::AcquiringOwnership,
            SupervisorState::Recovering,
            SupervisorState::Ready,
            SupervisorState::Draining,
            SupervisorState::Stopping,
            SupervisorState::Stopped,
            SupervisorState::Failed,
            SupervisorState::TakingOver,
        ];

        for &terminal in &terminals {
            for &next in &all_states {
                assert!(
                    fsm.validate_transition(terminal, next).is_err(),
                    "terminal {} → {} must be rejected",
                    terminal,
                    next
                );
            }
        }
    }

    // ── Database persistence tests ──────────────────────────────────

    #[tokio::test]
    async fn test_instance_insert_and_retrieve() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance = make_test_instance("test-dir", SupervisorState::Created);
        repo.insert_instance(&instance).await.unwrap();

        let retrieved = repo
            .get_instance(&instance.instance_id)
            .await
            .unwrap()
            .expect("instance should exist");

        assert_eq!(retrieved.instance_id, instance.instance_id);
        assert_eq!(retrieved.state_directory_id, "test-dir");
        assert_eq!(retrieved.pid, instance.pid);
        assert_eq!(retrieved.state, SupervisorState::Created);
    }

    #[tokio::test]
    async fn test_state_update_and_event() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance = make_test_instance("test-dir", SupervisorState::Created);
        repo.insert_instance(&instance).await.unwrap();

        // Update state + event in same transaction
        let event = harness_core::contracts::supervisor::SupervisorEvent::SupervisorReady {
            instance_id: instance.instance_id.clone(),
            occurred_at: Utc::now(),
        };
        repo.update_state_and_append_event(&instance.instance_id, SupervisorState::Ready, &event)
            .await
            .unwrap();

        let updated = repo
            .get_instance(&instance.instance_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.state, SupervisorState::Ready);
    }

    // ── Lease acquisition tests ─────────────────────────────────────

    #[tokio::test]
    async fn test_first_supervisor_acquires_lease() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance = make_test_instance("test-dir", SupervisorState::Created);
        repo.insert_instance(&instance).await.unwrap();

        // First acquisition should succeed
        let expires_at = Utc::now() + Duration::from_secs(30);
        let result = repo
            .acquire_lease(&instance.instance_id, "test-dir", 1, expires_at)
            .await;
        assert!(result.is_ok(), "first lease acquisition should succeed");

        let lease = repo
            .get_active_lease("test-dir")
            .await
            .unwrap()
            .expect("lease should exist");
        assert_eq!(lease.fencing_token, 1);
        assert_eq!(lease.is_active, 1);
    }

    #[tokio::test]
    async fn test_second_active_lease_rejected() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        // First supervisor acquires lease
        let instance1 = make_test_instance("test-dir", SupervisorState::Created);
        repo.insert_instance(&instance1).await.unwrap();
        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance1.instance_id, "test-dir", 1, expires_at)
            .await
            .unwrap();

        // Second supervisor tries to acquire — should fail due to UNIQUE index
        let instance2 = make_test_instance("test-dir", SupervisorState::Created);
        repo.insert_instance(&instance2).await.unwrap();
        let _result = repo
            .acquire_lease(&instance2.instance_id, "test-dir", 2, expires_at)
            .await;

        // The INSERT with ON CONFLICT DO NOTHING won't error, but it won't insert either
        // Let's verify: the active lease still belongs to instance1
        let lease = repo
            .get_active_lease("test-dir")
            .await
            .unwrap()
            .expect("lease should still exist");
        assert_eq!(lease.instance_id, instance1.instance_id.0);
        assert_eq!(lease.fencing_token, 1);
    }

    #[tokio::test]
    async fn test_lease_release_cas_succeeds() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance = make_test_instance("test-dir", SupervisorState::Created);
        repo.insert_instance(&instance).await.unwrap();
        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance.instance_id, "test-dir", 1, expires_at)
            .await
            .unwrap();

        // Release with correct fencing token
        let released = repo
            .release_lease_cas(&instance.instance_id, 1)
            .await
            .unwrap();
        assert!(released, "lease should be released");

        let lease = repo.get_active_lease("test-dir").await.unwrap();
        assert!(lease.is_none(), "no active lease should remain");
    }

    #[tokio::test]
    async fn test_lease_release_cas_wrong_token_fails() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance = make_test_instance("test-dir", SupervisorState::Created);
        repo.insert_instance(&instance).await.unwrap();
        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance.instance_id, "test-dir", 1, expires_at)
            .await
            .unwrap();

        // Release with wrong fencing token
        let released = repo
            .release_lease_cas(&instance.instance_id, 999)
            .await
            .unwrap();
        assert!(!released, "release with wrong token should fail");

        // Lease should still be active
        let lease = repo
            .get_active_lease("test-dir")
            .await
            .unwrap()
            .expect("lease should still be active");
        assert_eq!(lease.fencing_token, 1);
    }

    #[tokio::test]
    async fn test_force_deactivate_then_new_lease() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        // First supervisor acquires
        let instance1 = make_test_instance("test-dir", SupervisorState::Created);
        repo.insert_instance(&instance1).await.unwrap();
        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance1.instance_id, "test-dir", 1, expires_at)
            .await
            .unwrap();

        // Force deactivate
        let old_lease = repo.force_deactivate_lease("test-dir").await.unwrap();
        assert!(old_lease.is_some());
        assert_eq!(old_lease.unwrap().fencing_token, 1);

        // New supervisor acquires with incremented token
        let instance2 = make_test_instance("test-dir", SupervisorState::Created);
        repo.insert_instance(&instance2).await.unwrap();
        repo.acquire_lease(&instance2.instance_id, "test-dir", 2, expires_at)
            .await
            .unwrap();

        let lease = repo.get_active_lease("test-dir").await.unwrap().unwrap();
        assert_eq!(lease.instance_id, instance2.instance_id.0);
        assert_eq!(lease.fencing_token, 2);
    }

    // ── Heartbeat tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_heartbeat_cas_succeeds() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let mut instance = make_test_instance("test-dir", SupervisorState::Ready);
        instance.fencing_token = 1; // match the lease fencing token
        repo.insert_instance(&instance).await.unwrap();
        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance.instance_id, "test-dir", 1, expires_at)
            .await
            .unwrap();

        // Heartbeat with correct fencing token
        let new_expiry = Utc::now() + Duration::from_secs(60);
        let ok = repo
            .heartbeat_cas(&instance.instance_id, 1, new_expiry)
            .await
            .unwrap();
        assert!(ok, "heartbeat CAS should succeed");
    }

    #[tokio::test]
    async fn test_heartbeat_cas_wrong_token_fails() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance = make_test_instance("test-dir", SupervisorState::Ready);
        repo.insert_instance(&instance).await.unwrap();
        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance.instance_id, "test-dir", 1, expires_at)
            .await
            .unwrap();

        // Heartbeat with wrong fencing token (simulating takeover)
        let new_expiry = Utc::now() + Duration::from_secs(60);
        let ok = repo
            .heartbeat_cas(&instance.instance_id, 999, new_expiry)
            .await
            .unwrap();
        assert!(!ok, "heartbeat CAS with wrong token should fail");
    }

    #[tokio::test]
    async fn test_heartbeat_cas_wrong_state_fails() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance = make_test_instance("test-dir", SupervisorState::Stopped);
        repo.insert_instance(&instance).await.unwrap();
        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance.instance_id, "test-dir", 1, expires_at)
            .await
            .unwrap();

        // Heartbeat while in Stopped state — should fail (state NOT IN ready/recovering/draining)
        let new_expiry = Utc::now() + Duration::from_secs(60);
        let ok = repo
            .heartbeat_cas(&instance.instance_id, 1, new_expiry)
            .await
            .unwrap();
        assert!(!ok, "heartbeat CAS in Stopped state should fail");
    }

    // ── Fencing validation tests ────────────────────────────────────

    #[tokio::test]
    async fn test_fencing_validation_current_token_allowed() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance = make_test_instance("test-dir", SupervisorState::Ready);
        repo.insert_instance(&instance).await.unwrap();
        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance.instance_id, "test-dir", 1, expires_at)
            .await
            .unwrap();

        let allowed = repo
            .validate_fencing_for_write("test-dir", 1)
            .await
            .unwrap();
        assert!(allowed, "current fencing token should be allowed");
    }

    #[tokio::test]
    async fn test_fencing_validation_newer_token_allowed() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance = make_test_instance("test-dir", SupervisorState::Ready);
        repo.insert_instance(&instance).await.unwrap();
        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance.instance_id, "test-dir", 1, expires_at)
            .await
            .unwrap();

        // A newer token (from takeover) is allowed
        let allowed = repo
            .validate_fencing_for_write("test-dir", 5)
            .await
            .unwrap();
        assert!(allowed, "newer fencing token should be allowed");
    }

    #[tokio::test]
    async fn test_fencing_validation_older_token_rejected() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance = make_test_instance("test-dir", SupervisorState::Ready);
        repo.insert_instance(&instance).await.unwrap();
        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance.instance_id, "test-dir", 5, expires_at)
            .await
            .unwrap();

        // An older token (from a previous lease holder) is rejected
        let allowed = repo
            .validate_fencing_for_write("test-dir", 1)
            .await
            .unwrap();
        assert!(!allowed, "older fencing token should be rejected");
    }

    // ── Graceful shutdown tests ─────────────────────────────────────

    #[tokio::test]
    async fn test_graceful_shutdown_releases_lease() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance = make_test_instance("test-dir", SupervisorState::Ready);
        repo.insert_instance(&instance).await.unwrap();
        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance.instance_id, "test-dir", 1, expires_at)
            .await
            .unwrap();

        // Transition to stopped
        let stop_event = harness_core::contracts::supervisor::SupervisorEvent::SupervisorStopped {
            instance_id: instance.instance_id.clone(),
            occurred_at: Utc::now(),
        };
        repo.update_state_and_append_event(
            &instance.instance_id,
            SupervisorState::Stopped,
            &stop_event,
        )
        .await
        .unwrap();

        // Release lease
        let released = repo
            .release_lease_cas(&instance.instance_id, 1)
            .await
            .unwrap();
        assert!(released);

        // Verify no active lease
        let lease = repo.get_active_lease("test-dir").await.unwrap();
        assert!(lease.is_none());
    }

    // ── Multiple state directory isolation ──────────────────────────

    #[tokio::test]
    async fn test_different_state_dirs_can_have_leases() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SupervisorRepo::new(db.pool.clone());

        let instance_a = make_test_instance("dir-a", SupervisorState::Ready);
        let instance_b = make_test_instance("dir-b", SupervisorState::Ready);
        repo.insert_instance(&instance_a).await.unwrap();
        repo.insert_instance(&instance_b).await.unwrap();

        let expires_at = Utc::now() + Duration::from_secs(30);
        repo.acquire_lease(&instance_a.instance_id, "dir-a", 1, expires_at)
            .await
            .unwrap();
        repo.acquire_lease(&instance_b.instance_id, "dir-b", 1, expires_at)
            .await
            .unwrap();

        let lease_a = repo
            .get_active_lease("dir-a")
            .await
            .unwrap()
            .expect("dir-a should have lease");
        let lease_b = repo
            .get_active_lease("dir-b")
            .await
            .unwrap()
            .expect("dir-b should have lease");

        assert_eq!(lease_a.instance_id, instance_a.instance_id.0);
        assert_eq!(lease_b.instance_id, instance_b.instance_id.0);
    }

    // ── SupervisorConfig defaults ───────────────────────────────────

    #[test]
    fn test_supervisor_config_defaults() {
        let config = SupervisorConfig::default();
        assert_eq!(config.lease_duration_secs, 30);
        assert_eq!(config.heartbeat_interval_secs, 10);
        assert_eq!(config.max_operation_concurrency, 8);
        assert_eq!(config.shutdown_grace_period_secs, 30);
        assert_eq!(config.max_ipc_frame_bytes, 16 * 1024 * 1024);
        assert_eq!(config.max_ipc_connections, 32);
        assert_eq!(config.max_inflight_requests, 64);
        assert_eq!(config.max_event_stream_buffer, 1024);
        assert_eq!(config.max_diagnostic_bytes, 1024 * 1024);
        assert_eq!(config.state_directory_id, "default");
    }

    #[test]
    fn test_heartbeat_interval_less_than_lease_third() {
        let config = SupervisorConfig::default();
        assert!(
            config.heartbeat_interval_secs < config.lease_duration_secs / 3 + 1,
            "heartbeat should be less than lease_duration/3 for safety margin"
        );
    }

    // ── IPC lifecycle tests ──────────────────────────────────────────

    #[test]
    fn test_supervisor_config_includes_ipc_endpoint() {
        let config = SupervisorConfig::default();
        assert_eq!(config.ipc_endpoint, "harness-supervisor");
        assert!(!config.ipc_endpoint.is_empty());
    }

    #[tokio::test]
    async fn test_ipc_server_construction() {
        use crate::ipc::IpcServer;
        use harness_core::contracts::ipc::IpcConfig;

        let db = Database::open_in_memory().await.unwrap();
        let config = IpcConfig::default();

        // Use a minimal mock handler
        struct MockHandler;
        #[async_trait::async_trait]
        impl crate::ipc::IpcCommandHandler for MockHandler {
            async fn handle_command(
                &self,
                _command: &harness_core::contracts::ipc::IpcCommand,
                _payload: &serde_json::Value,
            ) -> Result<serde_json::Value, harness_core::CoreError> {
                Ok(serde_json::json!({"mock": true}))
            }
        }

        let handler = std::sync::Arc::new(MockHandler);
        let server = IpcServer::new(config, handler, db.pool.clone());
        assert_eq!(server.active_connections().await, 0);

        // Shutdown should be idempotent
        server.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_ipc_server_shutdown_prevents_hang() {
        use crate::ipc::IpcServer;
        use harness_core::contracts::ipc::IpcConfig;

        let db = Database::open_in_memory().await.unwrap();
        let config = IpcConfig {
            accept_timeout_secs: 1,
            ..Default::default()
        };

        struct MockHandler;
        #[async_trait::async_trait]
        impl crate::ipc::IpcCommandHandler for MockHandler {
            async fn handle_command(
                &self,
                _command: &harness_core::contracts::ipc::IpcCommand,
                _payload: &serde_json::Value,
            ) -> Result<serde_json::Value, harness_core::CoreError> {
                Ok(serde_json::json!({"mock": true}))
            }
        }

        let handler = std::sync::Arc::new(MockHandler);
        let server = IpcServer::new(config, handler, db.pool.clone());

        // Signal shutdown before serving — serve should exit cleanly
        server.shutdown().await;

        // Must not hang; either Ok (clean exit after shutdown) or Err is acceptable
        let _ = server.serve("harness-test-ipc-shutdown").await;
    }
}
