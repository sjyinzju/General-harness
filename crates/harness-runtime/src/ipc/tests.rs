//! I6.2 IPC protocol and framing tests.

#[cfg(test)]
mod ipc_tests {
    use chrono::Utc;
    use harness_core::contracts::ipc::{
        IpcCommand, IpcRequestEnvelope, IpcResponseEnvelope, IpcResponseStatus, StructuredIpcError,
        IPC_PROTOCOL_VERSION,
    };

    // ── Command parsing ──────────────────────────────────────────

    #[test]
    fn test_command_whitelist() {
        // Valid commands
        assert!(IpcCommand::parse("supervisor.status").is_some());
        assert!(IpcCommand::parse("supervisor.stop").is_some());
        assert!(IpcCommand::parse("task.start").is_some());
        assert!(IpcCommand::parse("task.status").is_some());
        assert!(IpcCommand::parse("review.create").is_some());
        assert!(IpcCommand::parse("review.run").is_some());
        assert!(IpcCommand::parse("integration.enqueue").is_some());
        assert!(IpcCommand::parse("integration.run_next").is_some());
        assert!(IpcCommand::parse("subscribe").is_some());
        assert!(IpcCommand::parse("health").is_some());
        assert!(IpcCommand::parse("diagnostics").is_some());
        assert!(IpcCommand::parse("cancel").is_some());
        assert!(IpcCommand::parse("inspect").is_some());

        // Invalid commands
        assert!(IpcCommand::parse("").is_none());
        assert!(IpcCommand::parse("unknown.command").is_none());
        assert!(IpcCommand::parse("malicious").is_none());
        assert!(IpcCommand::parse("supervisor.run").is_none());
    }

    #[test]
    fn test_command_side_effects() {
        // Read-only commands
        assert!(!IpcCommand::parse("supervisor.status")
            .unwrap()
            .has_side_effects());
        assert!(!IpcCommand::parse("task.status").unwrap().has_side_effects());
        assert!(!IpcCommand::parse("review.show").unwrap().has_side_effects());
        assert!(!IpcCommand::parse("review.list").unwrap().has_side_effects());
        assert!(!IpcCommand::parse("integration.show")
            .unwrap()
            .has_side_effects());
        assert!(!IpcCommand::parse("integration.list")
            .unwrap()
            .has_side_effects());
        assert!(!IpcCommand::parse("health").unwrap().has_side_effects());
        assert!(!IpcCommand::parse("diagnostics").unwrap().has_side_effects());

        // Write commands
        assert!(IpcCommand::parse("task.start").unwrap().has_side_effects());
        assert!(IpcCommand::parse("task.resume").unwrap().has_side_effects());
        assert!(IpcCommand::parse("task.cancel").unwrap().has_side_effects());
        assert!(IpcCommand::parse("review.create")
            .unwrap()
            .has_side_effects());
        assert!(IpcCommand::parse("review.run").unwrap().has_side_effects());
        assert!(IpcCommand::parse("integration.enqueue")
            .unwrap()
            .has_side_effects());
        assert!(IpcCommand::parse("integration.run_next")
            .unwrap()
            .has_side_effects());
        assert!(IpcCommand::parse("integration.cancel")
            .unwrap()
            .has_side_effects());
        assert!(IpcCommand::parse("integration.recover")
            .unwrap()
            .has_side_effects());
        assert!(IpcCommand::parse("cancel").unwrap().has_side_effects());
    }

    #[test]
    fn test_command_round_trip() {
        for cmd in &[
            "supervisor.status",
            "supervisor.stop",
            "task.start",
            "task.status",
            "task.resume",
            "task.cancel",
            "task.inspect",
            "task.dry_run_decision",
            "review.create",
            "review.run",
            "review.show",
            "review.list",
            "integration.enqueue",
            "integration.run_next",
            "integration.show",
            "integration.list",
            "integration.cancel",
            "integration.recover",
            "inspect",
            "cancel",
            "subscribe",
            "unsubscribe",
            "health",
            "diagnostics",
        ] {
            let parsed = IpcCommand::parse(cmd).expect("valid command");
            assert_eq!(parsed.as_str(), *cmd, "round-trip failed for {cmd}");
        }
    }

    // ── Request/Response serialization ─────────────────────────

    #[test]
    fn test_request_serialization() {
        let req = IpcRequestEnvelope {
            protocol_version: IPC_PROTOCOL_VERSION.to_string(),
            request_id: "req-1".to_string(),
            idempotency_key: "idem-1".to_string(),
            command: "supervisor.status".to_string(),
            payload: serde_json::json!({"state_dir": "default"}),
            client_pid: 12345,
            sent_at: Utc::now(),
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: IpcRequestEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.request_id, "req-1");
        assert_eq!(parsed.command, "supervisor.status");
        assert_eq!(parsed.client_pid, 12345);
    }

    #[test]
    fn test_response_success_serialization() {
        let resp = IpcResponseEnvelope {
            protocol_version: IPC_PROTOCOL_VERSION.to_string(),
            request_id: "req-1".to_string(),
            status: IpcResponseStatus::Success,
            payload: Some(serde_json::json!({"state": "ready"})),
            error: None,
            completed_at: Utc::now(),
        };

        let json = serde_json::to_string(&resp).unwrap();
        let parsed: IpcResponseEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.status, IpcResponseStatus::Success);
        assert!(parsed.payload.is_some());
        assert!(parsed.error.is_none());
    }

    #[test]
    fn test_response_error_serialization() {
        let resp = IpcResponseEnvelope {
            protocol_version: IPC_PROTOCOL_VERSION.to_string(),
            request_id: "req-2".to_string(),
            status: IpcResponseStatus::Error,
            payload: None,
            error: Some(StructuredIpcError {
                code: "not_found".to_string(),
                message: "task not found".to_string(),
                details: Some(serde_json::json!({"task_id": "t-123"})),
            }),
            completed_at: Utc::now(),
        };

        let json = serde_json::to_string(&resp).unwrap();
        let parsed: IpcResponseEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.status, IpcResponseStatus::Error);
        assert!(parsed.error.is_some());
        assert_eq!(parsed.error.unwrap().code, "not_found");
    }

    #[test]
    fn test_response_status_all_variants() {
        let variants = [
            IpcResponseStatus::Success,
            IpcResponseStatus::Accepted,
            IpcResponseStatus::BadRequest,
            IpcResponseStatus::Rejected,
            IpcResponseStatus::Error,
            IpcResponseStatus::Duplicate,
            IpcResponseStatus::Conflict,
            IpcResponseStatus::NotReady,
        ];

        for variant in &variants {
            let resp = IpcResponseEnvelope {
                protocol_version: "1.0".to_string(),
                request_id: "test".to_string(),
                status: variant.clone(),
                payload: None,
                error: None,
                completed_at: Utc::now(),
            };

            let json = serde_json::to_string(&resp).unwrap();
            let parsed: IpcResponseEnvelope = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed.status, *variant);
        }
    }

    // ── Protocol version validation ──────────────────────────────

    #[test]
    fn test_protocol_version_constant() {
        assert_eq!(IPC_PROTOCOL_VERSION, "1.0");
    }

    #[test]
    fn test_version_mismatch_detected_in_request() {
        let req_json = serde_json::json!({
            "protocol_version": "0.9",
            "request_id": "test",
            "idempotency_key": "key",
            "command": "health",
            "payload": {},
            "client_pid": 1,
            "sent_at": "2026-01-01T00:00:00Z"
        });

        let req: IpcRequestEnvelope = serde_json::from_value(req_json).unwrap();
        assert_ne!(req.protocol_version, IPC_PROTOCOL_VERSION);
    }

    // ── Frame length encoding ────────────────────────────────────

    #[test]
    fn test_length_prefix_round_trip() {
        use harness_core::contracts::ipc::FRAME_LENGTH_PREFIX_BYTES;

        // Encode a small frame
        let payload = b"hello world";
        let len = payload.len() as u32;
        let len_bytes = len.to_be_bytes();

        assert_eq!(len_bytes.len(), FRAME_LENGTH_PREFIX_BYTES);

        // Decode
        let decoded_len = u32::from_be_bytes(len_bytes);
        assert_eq!(decoded_len as usize, payload.len());
    }

    #[test]
    fn test_max_frame_size_sanity() {
        use harness_core::contracts::ipc::DEFAULT_MAX_FRAME_BYTES;
        assert_eq!(DEFAULT_MAX_FRAME_BYTES, 16 * 1024 * 1024);
        // Must be representable in u32 (4-byte length prefix)
        assert!(DEFAULT_MAX_FRAME_BYTES <= u32::MAX as usize);
    }

    // ── Config defaults ──────────────────────────────────────────

    #[test]
    fn test_ipc_config_defaults() {
        use harness_core::contracts::ipc::IpcConfig;

        let config = IpcConfig::default();
        assert_eq!(config.max_frame_bytes, 16 * 1024 * 1024);
        assert_eq!(config.max_connections, 32);
        assert_eq!(config.max_inflight_requests, 64);
        assert_eq!(config.read_timeout_secs, 30);
        assert_eq!(config.write_timeout_secs, 30);
    }

    // ── IpcRequestState ──────────────────────────────────────────

    #[test]
    fn test_request_state_terminal() {
        use harness_core::contracts::ipc::IpcRequestState;

        assert!(IpcRequestState::Completed.is_terminal());
        assert!(IpcRequestState::Rejected.is_terminal());
        assert!(IpcRequestState::Failed.is_terminal());
        assert!(IpcRequestState::Cancelled.is_terminal());

        assert!(!IpcRequestState::Received.is_terminal());
        assert!(!IpcRequestState::Persisted.is_terminal());
        assert!(!IpcRequestState::Dispatching.is_terminal());
    }
}
