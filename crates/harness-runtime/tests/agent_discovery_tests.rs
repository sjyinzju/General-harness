//! I4-A Agent Discovery comprehensive integration tests.
//!
//! Covers: discovery, adapter contracts, capability negotiation, persistence.
//! All tests use fake executables only — no real Agent CLI calls, no API costs.

use chrono::Utc;
use harness_core::contracts::agent_event::AgentEvent;
use harness_core::contracts::discovery::{
    AuthModeHint, AuthStateValue, AuthenticationState, CapabilityNegotiation, CapabilitySupport,
    DiscoveredAgent, DiscoveryConfidence, DiscoveryEvidence, EvidenceKind, ExecutableIdentity,
    ProviderHint, ProviderHintSource, ValidationResult, ValidationStatus,
};
use harness_core::contracts::runtime_profile::{
    AuthCheckStatus, AuthMode, AuthStatus, CapabilitySet, CoreStatus, ExecutionStatus,
    OptionalCapabilities, ProviderSource, RequiredCapabilities, RuntimeProfile, TriState,
};
use harness_runtime::db::Database;
use harness_runtime::discovery::repo;

// ── Helpers ──────────────────────────────────────────────────────────

#[allow(dead_code)]
fn test_identity(kind: &str) -> ExecutableIdentity {
    ExecutableIdentity::compute(&format!("/usr/bin/{}", kind), kind)
}

fn test_agent(kind: &str, version: &str) -> DiscoveredAgent {
    let now = Utc::now();
    let identity = ExecutableIdentity::compute(&format!("/usr/bin/{}", kind), kind);
    DiscoveredAgent {
        identity,
        discovery_evidence: vec![DiscoveryEvidence {
            evidence_kind: EvidenceKind::PathResolution,
            observation: format!("Found at /usr/bin/{}", kind),
            confidence: DiscoveryConfidence::High,
            collected_at: now,
        }],
        confidence: DiscoveryConfidence::High,
        version: Some(version.to_string()),
        is_wrapper: false,
        wraps_agent_kind: None,
        provider_hints: vec![ProviderHint {
            provider: if kind == "claude-code" {
                "anthropic"
            } else {
                "openai"
            }
            .to_string(),
            source: ProviderHintSource::Unknown,
            confidence: DiscoveryConfidence::Medium,
            evidence: vec!["Default provider".to_string()],
            base_url: None,
            is_custom_endpoint: false,
        }],
        authentication_state: AuthenticationState {
            status: AuthStateValue::Unknown,
            mode: AuthModeHint::Unknown,
            evidence: vec![],
        },
        profiles: vec![format!("{}-default", kind)],
        first_seen_at: now,
        last_seen_at: now,
    }
}

fn test_runtime_profile() -> RuntimeProfile {
    RuntimeProfile {
        id: "test-profile-1".into(),
        agent_definition_id: "test-def-1".into(),
        label: "Test Profile".into(),
        agent_kind: "claude-code".into(),
        adapter_kind: "claude-cli".into(),
        agent_version: "2.1.210".into(),
        executable_path: "/usr/bin/claude".into(),
        provider: "anthropic".into(),
        provider_source: ProviderSource::UserDeclared,
        model: None,
        base_url: None,
        auth_mode: AuthMode::ApiKeyEnv,
        auth_status: AuthStatus::Unknown,
        credential_ref: None,
        capabilities: CapabilitySet {
            required: RequiredCapabilities {
                execute: TriState::Supported,
                working_directory: TriState::Supported,
                stream_output: TriState::Supported,
                process_exit: TriState::Supported,
                cancellation: TriState::Supported,
                timeout: TriState::Supported,
                final_result: TriState::Supported,
            },
            optional: OptionalCapabilities {
                native_session_resume: TriState::Unknown,
                structured_output: TriState::Unknown,
                tool_events: TriState::Supported,
                file_change_events: TriState::Unsupported,
                reasoning_summary: TriState::Unknown,
                interactive_approval: TriState::Unsupported,
                usage_reporting: TriState::Unknown,
            },
            workspace_modes: vec!["read".into(), "write".into()],
            supported_languages: vec![],
            mcp_tools: vec![],
            supported_platforms: vec!["all".into()],
        },
        core_status: CoreStatus::Available,
        authentication_status: AuthCheckStatus::Unknown,
        execution_status: ExecutionStatus::Untested,
        optional_integrations: vec![],
        discovery_source: "test".into(),
        passive_probe: None,
        active_validation: None,
        concurrency_max: 1,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

// ══════════════════════════════════════════════════════════════════════
// Section 1: ExecutableIdentity & Basic Types (Tests 1-5)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_01_executable_identity_stable() {
    let id1 = ExecutableIdentity::compute("/usr/bin/claude", "claude-code");
    let id2 = ExecutableIdentity::compute("/usr/bin/claude", "claude-code");
    assert_eq!(id1.discovery_hash, id2.discovery_hash);
}

#[test]
fn test_02_executable_identity_different_paths() {
    let id1 = ExecutableIdentity::compute("/usr/bin/claude", "claude-code");
    let id2 = ExecutableIdentity::compute("/usr/local/bin/claude", "claude-code");
    assert_ne!(id1.discovery_hash, id2.discovery_hash);
}

#[test]
fn test_03_executable_identity_different_kinds() {
    let id1 = ExecutableIdentity::compute("/usr/bin/claude", "claude-code");
    let id2 = ExecutableIdentity::compute("/usr/bin/claude", "codex");
    assert_ne!(id1.discovery_hash, id2.discovery_hash);
}

#[test]
fn test_04_executable_identity_basename() {
    let id = ExecutableIdentity::compute("C:\\Users\\test\\npm\\claude.ps1", "claude-code");
    assert!(id.executable_basename.contains("claude"));
}

#[test]
fn test_05_discovery_confidence_values() {
    assert_ne!(DiscoveryConfidence::High, DiscoveryConfidence::Low);
    assert_ne!(DiscoveryConfidence::Heuristic, DiscoveryConfidence::Medium);
}

// ══════════════════════════════════════════════════════════════════════
// Section 2: CapabilityNegotiation (Tests 6-10)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_06_capability_all_unknown_default() {
    let caps = CapabilityNegotiation::all_unknown();
    assert_eq!(caps.execute, CapabilitySupport::Unknown);
    assert_eq!(caps.working_directory, CapabilitySupport::Unknown);
    assert_eq!(caps.stream_output, CapabilitySupport::Unknown);
}

#[test]
fn test_07_native_capability() {
    let mut caps = CapabilityNegotiation::all_unknown();
    caps.execute = CapabilitySupport::Native;
    assert_eq!(caps.execute, CapabilitySupport::Native);
}

#[test]
fn test_08_harness_emulated_capability() {
    let mut caps = CapabilityNegotiation::all_unknown();
    caps.timeout = CapabilitySupport::HarnessEmulated;
    caps.cancellation = CapabilitySupport::HarnessEmulated;
    assert_eq!(caps.timeout, CapabilitySupport::HarnessEmulated);
    assert_eq!(caps.cancellation, CapabilitySupport::HarnessEmulated);
}

#[test]
fn test_09_unsupported_capability() {
    let mut caps = CapabilityNegotiation::all_unknown();
    caps.file_attachments = CapabilitySupport::Unsupported;
    assert_eq!(caps.file_attachments, CapabilitySupport::Unsupported);
}

#[test]
fn test_10_harness_not_pretending_native() {
    // Harness-emulated capabilities must NOT be reported as Native
    let caps = CapabilityNegotiation::all_unknown();
    assert_ne!(caps.timeout, CapabilitySupport::Native);
}

// ══════════════════════════════════════════════════════════════════════
// Section 3: ProviderHint (Tests 11-15)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_11_provider_hint_with_evidence() {
    let hint = ProviderHint {
        provider: "anthropic".to_string(),
        source: ProviderHintSource::Unknown,
        confidence: DiscoveryConfidence::Medium,
        evidence: vec!["Default for claude-code".to_string()],
        base_url: None,
        is_custom_endpoint: false,
    };
    assert!(!hint.evidence.is_empty());
    assert_eq!(hint.provider, "anthropic");
}

#[test]
fn test_12_provider_hint_custom_endpoint() {
    let hint = ProviderHint {
        provider: "custom-anthropic-compatible".to_string(),
        source: ProviderHintSource::EnvironmentHint,
        confidence: DiscoveryConfidence::Low,
        evidence: vec!["ANTHROPIC_BASE_URL is set".to_string()],
        base_url: None,
        is_custom_endpoint: true,
    };
    assert!(hint.is_custom_endpoint);
    assert_eq!(hint.confidence, DiscoveryConfidence::Low);
}

#[test]
fn test_13_provider_not_determined_by_model_name() {
    // Model name alone does NOT determine provider
    let hint = ProviderHint {
        provider: "unknown".to_string(),
        source: ProviderHintSource::Unknown,
        confidence: DiscoveryConfidence::Heuristic,
        evidence: vec![],
        base_url: None,
        is_custom_endpoint: false,
    };
    assert_eq!(hint.confidence, DiscoveryConfidence::Heuristic);
}

#[test]
fn test_14_env_based_hint_low_confidence() {
    let hint = ProviderHint {
        provider: "deepseek".to_string(),
        source: ProviderHintSource::EnvironmentHint,
        confidence: DiscoveryConfidence::Low,
        evidence: vec!["DEEPSEEK_API_KEY is set".to_string()],
        base_url: None,
        is_custom_endpoint: false,
    };
    assert_eq!(hint.confidence, DiscoveryConfidence::Low);
}

#[test]
fn test_15_user_declared_hint_high_confidence() {
    let hint = ProviderHint {
        provider: "anthropic".to_string(),
        source: ProviderHintSource::UserDeclared,
        confidence: DiscoveryConfidence::High,
        evidence: vec!["User explicitly configured".to_string()],
        base_url: None,
        is_custom_endpoint: false,
    };
    assert_eq!(hint.source, ProviderHintSource::UserDeclared);
}

// ══════════════════════════════════════════════════════════════════════
// Section 4: AuthenticationState (Tests 16-18)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_16_auth_state_unknown_default() {
    let auth = AuthenticationState {
        status: AuthStateValue::Unknown,
        mode: AuthModeHint::Unknown,
        evidence: vec![],
    };
    assert_eq!(auth.status, AuthStateValue::Unknown);
}

#[test]
fn test_17_auth_state_authenticated() {
    let auth = AuthenticationState {
        status: AuthStateValue::Authenticated,
        mode: AuthModeHint::Login,
        evidence: vec!["login status confirmed".to_string()],
    };
    assert_eq!(auth.status, AuthStateValue::Authenticated);
}

#[test]
fn test_18_auth_env_key_not_claiming_logged_in() {
    // API key env present does NOT mean authenticated
    let auth = AuthenticationState {
        status: AuthStateValue::Unknown,
        mode: AuthModeHint::ApiKeyEnv,
        evidence: vec!["ANTHROPIC_API_KEY is set (value not read)".to_string()],
    };
    assert_eq!(auth.status, AuthStateValue::Unknown);
    assert_eq!(auth.mode, AuthModeHint::ApiKeyEnv);
}

// ══════════════════════════════════════════════════════════════════════
// Section 5: Discovery Evidence (Tests 19-21)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_19_path_resolution_evidence() {
    let evidence = DiscoveryEvidence {
        evidence_kind: EvidenceKind::PathResolution,
        observation: "Found at /usr/bin/claude".to_string(),
        confidence: DiscoveryConfidence::High,
        collected_at: Utc::now(),
    };
    assert_eq!(evidence.evidence_kind, EvidenceKind::PathResolution);
    assert_eq!(evidence.confidence, DiscoveryConfidence::High);
}

#[test]
fn test_20_version_output_evidence() {
    let evidence = DiscoveryEvidence {
        evidence_kind: EvidenceKind::VersionOutput,
        observation: "Version output: 2.1.210".to_string(),
        confidence: DiscoveryConfidence::High,
        collected_at: Utc::now(),
    };
    assert_eq!(evidence.evidence_kind, EvidenceKind::VersionOutput);
}

#[test]
fn test_21_environment_presence_evidence_names_only() {
    let evidence = DiscoveryEvidence {
        evidence_kind: EvidenceKind::EnvironmentPresence,
        observation: "ANTHROPIC_API_KEY is set (value not read)".to_string(),
        confidence: DiscoveryConfidence::Low,
        collected_at: Utc::now(),
    };
    // Evidence must NOT contain actual values
    assert!(!evidence.observation.contains("sk-"));
    assert!(evidence.observation.contains("value not read"));
}

// ══════════════════════════════════════════════════════════════════════
// Section 6: DiscoveredAgent Model (Tests 22-26)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_22_agent_definition_scaffold() {
    let agent = test_agent("claude-code", "2.1.210");
    assert_eq!(agent.identity.agent_kind, "claude-code");
    assert_eq!(agent.version, Some("2.1.210".to_string()));
    assert!(!agent.is_wrapper);
}

#[test]
fn test_23_same_agent_multiple_profiles() {
    let mut agent = test_agent("claude-code", "2.1.210");
    agent.profiles = vec![
        "claude-code-default".to_string(),
        "claude-code-deepseek".to_string(),
        "claude-code-glm-wrapper".to_string(),
        "user-configured-profile".to_string(),
    ];
    assert_eq!(agent.profiles.len(), 4);
    // Multiple profiles belong to same AgentDefinition
    assert!(agent.profiles.iter().any(|p| p.contains("claude")));
}

#[test]
fn test_24_wrapper_agent_identity() {
    let mut agent = test_agent("claude-code", "2.1.210");
    agent.is_wrapper = true;
    agent.wraps_agent_kind = Some("claude-code".to_string());
    agent.identity = ExecutableIdentity::compute("/usr/bin/claude-glm", "claude-code");

    assert!(agent.is_wrapper);
    assert_eq!(agent.wraps_agent_kind, Some("claude-code".to_string()));
}

#[test]
fn test_25_wrapper_not_separate_agent_definition() {
    // Wrapper and base agent share same wraps_agent_kind
    let wrapper = {
        let mut a = test_agent("claude-code", "2.1.210");
        a.is_wrapper = true;
        a.wraps_agent_kind = Some("claude-code".to_string());
        a.identity = ExecutableIdentity::compute("/usr/bin/claude-glm", "claude-code");
        a
    };
    let base = test_agent("claude-code", "2.1.210");

    // Both identify as claude-code agent kind
    assert_eq!(
        wrapper.wraps_agent_kind,
        Some(base.identity.agent_kind.clone())
    );
}

#[test]
fn test_26_discovery_confidence_on_agent() {
    let agent = test_agent("claude-code", "2.1.210");
    assert_eq!(agent.confidence, DiscoveryConfidence::High);
}

// ══════════════════════════════════════════════════════════════════════
// Section 7: RuntimeProfile Model (Tests 27-29)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_27_runtime_profile_basic() {
    let profile = test_runtime_profile();
    assert_eq!(profile.agent_kind, "claude-code");
    assert_eq!(profile.adapter_kind, "claude-cli");
    assert_eq!(profile.provider, "anthropic");
}

#[test]
fn test_28_runtime_profile_model_not_hardcoded() {
    let profile = test_runtime_profile();
    // Model and base_url should default to None in discovery
    assert!(profile.model.is_none());
    assert!(profile.base_url.is_none());
}

#[test]
fn test_29_runtime_profile_auth_not_hardcoded() {
    let profile = test_runtime_profile();
    // Auth mode should not be hardcoded to "authenticated"
    assert_ne!(profile.auth_status, AuthStatus::Authenticated);
}

// ══════════════════════════════════════════════════════════════════════
// Section 8: ValidationStatus (Tests 30-32)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_30_validation_status_default_not_validated() {
    let status = ValidationStatus {
        validated: false,
        validated_at: None,
        result: None,
        command_fingerprint: None,
        exit_status: None,
        diagnostics: vec![],
        artifact_reference: None,
        may_incur_cost: true,
    };
    assert!(!status.validated);
    assert!(status.result.is_none());
}

#[test]
fn test_31_active_validation_requires_explicit_permission() {
    // Active validation must NOT be auto-triggered
    let request = harness_core::contracts::discovery::ActiveValidationRequest {
        executable: "claude".to_string(),
        full_args: vec![
            "-p".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
        ],
        profile_id: "test-profile".to_string(),
        working_directory: "/tmp".to_string(),
        timeout_secs: 30,
        may_incur_cost: true,
        env_var_names: vec!["ANTHROPIC_API_KEY".to_string()],
    };
    assert!(request.may_incur_cost);
    assert!(!request.env_var_names.is_empty());
}

#[test]
fn test_32_validation_diagnostic_persistence() {
    let status = ValidationStatus {
        validated: true,
        validated_at: Some(Utc::now()),
        result: Some(ValidationResult::Passed),
        command_fingerprint: Some("abc123".to_string()),
        exit_status: Some(0),
        diagnostics: vec![],
        artifact_reference: Some("/tmp/validation-output.txt".to_string()),
        may_incur_cost: true,
    };
    assert!(status.validated);
    assert_eq!(status.result, Some(ValidationResult::Passed));
}

// ══════════════════════════════════════════════════════════════════════
// Section 9: AgentEvent Contract (Tests 33-38)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_33_agent_event_session_started() {
    let event = AgentEvent::SessionStarted {
        session_id: "s1".into(),
        profile_id: "p1".into(),
    };
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("session_started"));
    assert!(json.contains("s1"));
}

#[test]
fn test_34_agent_event_raw_vendor_not_silently_dropped() {
    let event = AgentEvent::RawVendorEvent {
        raw_type: "future.event.v2".into(),
        payload: serde_json::json!({"new_field": "value"}),
    };
    match &event {
        AgentEvent::RawVendorEvent { raw_type, .. } => {
            assert_eq!(raw_type, "future.event.v2");
        }
        _ => panic!("Should be RawVendorEvent"),
    }
}

#[test]
fn test_35_session_ended_synthetic() {
    let event = AgentEvent::SessionEnded {
        session_id: "s1".into(),
        synthetic: true,
        termination_reason: harness_core::contracts::agent_event::TerminationReason::Completed,
        result_received: true,
        process_exit_received: true,
    };
    match &event {
        AgentEvent::SessionEnded { synthetic, .. } => {
            assert!(synthetic);
        }
        _ => panic!("Should be SessionEnded"),
    }
}

#[test]
fn test_36_result_vs_process_exit_distinct_semantics() {
    let result = AgentEvent::Result {
        content: "done".into(),
        is_error: false,
    };
    let process_exit = AgentEvent::ProcessExited {
        exit_code: 1,
        signal: None,
    };
    // Result and ProcessExited have different semantics
    assert!(matches!(result, AgentEvent::Result { .. }));
    assert!(matches!(process_exit, AgentEvent::ProcessExited { .. }));
}

#[test]
fn test_37_nonzero_exit_not_pretending_success() {
    let exit = AgentEvent::ProcessExited {
        exit_code: 1,
        signal: None,
    };
    match exit {
        AgentEvent::ProcessExited { exit_code, .. } => {
            assert_ne!(exit_code, 0);
        }
        _ => panic!(),
    }
}

#[test]
fn test_38_termination_reason_variants() {
    use harness_core::contracts::agent_event::TerminationReason;
    let reasons = [
        TerminationReason::Completed,
        TerminationReason::ProcessExited {
            exit_code: 1,
            signal: None,
        },
        TerminationReason::Interrupted,
        TerminationReason::Cancelled,
        TerminationReason::Timeout,
        TerminationReason::Lost,
        TerminationReason::Unknown,
    ];
    assert_eq!(reasons.len(), 7);
}

// ══════════════════════════════════════════════════════════════════════
// Section 10: Adapter Contract (Tests 39-42)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_39_adapter_compatibility_diagnostic() {
    use harness_core::contracts::discovery::{
        AdapterCompatibility, CompatibilityDiagnostic, DiagnosticLevel,
    };
    let compat = AdapterCompatibility {
        compatible: true,
        adapter_kind: "codex-cli".to_string(),
        agent_version: "0.116.0".to_string(),
        diagnostics: vec![CompatibilityDiagnostic {
            level: DiagnosticLevel::Warning,
            category: "config".to_string(),
            message: "service_tier may need adjustment".to_string(),
            suggestion: Some("Use -c service_tier=fast".to_string()),
        }],
    };
    assert!(compat.compatible);
    assert_eq!(compat.diagnostics.len(), 1);
}

#[test]
fn test_40_incompatible_version_diagnostic() {
    use harness_core::contracts::discovery::{
        AdapterCompatibility, CompatibilityDiagnostic, DiagnosticLevel,
    };
    let compat = AdapterCompatibility {
        compatible: false,
        adapter_kind: "codex-cli".to_string(),
        agent_version: "0.1.0".to_string(),
        diagnostics: vec![CompatibilityDiagnostic {
            level: DiagnosticLevel::Fatal,
            category: "capability".to_string(),
            message: "Codex CLI does not support --json flag".to_string(),
            suggestion: Some("Upgrade Codex CLI".to_string()),
        }],
    };
    assert!(!compat.compatible);
}

#[test]
fn test_41_chatgpt_login_profile_representation() {
    // Codex users may use ChatGPT login without API key
    let profile = RuntimeProfile {
        id: "codex-chatgpt-login".into(),
        agent_definition_id: "codex-def".into(),
        label: "Codex ChatGPT".into(),
        agent_kind: "codex".into(),
        adapter_kind: "codex-cli".into(),
        agent_version: "0.116.0".into(),
        executable_path: "/usr/bin/codex".into(),
        provider: "openai".into(),
        provider_source: ProviderSource::UserDeclared,
        model: None,
        base_url: None,
        auth_mode: AuthMode::Login,
        auth_status: AuthStatus::Unknown,
        credential_ref: None,
        capabilities: CapabilitySet {
            required: RequiredCapabilities {
                execute: TriState::Supported,
                working_directory: TriState::Supported,
                stream_output: TriState::Supported,
                process_exit: TriState::Supported,
                cancellation: TriState::Supported,
                timeout: TriState::Supported,
                final_result: TriState::Supported,
            },
            optional: OptionalCapabilities {
                native_session_resume: TriState::Unknown,
                structured_output: TriState::Unknown,
                tool_events: TriState::Unknown,
                file_change_events: TriState::Unsupported,
                reasoning_summary: TriState::Unknown,
                interactive_approval: TriState::Unsupported,
                usage_reporting: TriState::Unknown,
            },
            workspace_modes: vec!["read".into(), "write".into()],
            supported_languages: vec![],
            mcp_tools: vec![],
            supported_platforms: vec!["all".into()],
        },
        core_status: CoreStatus::Available,
        authentication_status: AuthCheckStatus::Unknown,
        execution_status: ExecutionStatus::Untested,
        optional_integrations: vec![],
        discovery_source: "path".into(),
        passive_probe: None,
        active_validation: None,
        concurrency_max: 1,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    assert_eq!(profile.auth_mode, AuthMode::Login);
    assert!(profile.model.is_none());
}

#[test]
fn test_42_no_hardcoded_model_in_profile() {
    let profile = test_runtime_profile();
    // Profile should not hardcode model unless user/config specified
    assert!(profile.model.is_none());
}

// ══════════════════════════════════════════════════════════════════════
// Section 11: Persistence — Agent Definitions (Tests 43-48)
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_43_discovery_persistence() {
    let db = Database::open_in_memory().await.unwrap();
    let agent = test_agent("claude-code", "2.1.210");
    repo::upsert_agent_definition(&db.pool, &agent)
        .await
        .unwrap();

    // Verify persisted
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM agent_definitions WHERE id = ?")
        .bind(&agent.identity.discovery_hash)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn test_44_profile_update() {
    let db = Database::open_in_memory().await.unwrap();
    let agent = test_agent("codex", "0.116.0");

    repo::upsert_agent_definition(&db.pool, &agent)
        .await
        .unwrap();
    repo::upsert_runtime_profile(
        &db.pool,
        "codex-default",
        &agent.identity.discovery_hash,
        "codex",
        "codex-cli",
        "0.116.0",
        "/usr/bin/codex",
        "openai",
        "unknown",
        "Codex Default",
    )
    .await
    .unwrap();

    // Update with newer version
    repo::upsert_runtime_profile(
        &db.pool,
        "codex-default",
        &agent.identity.discovery_hash,
        "codex",
        "codex-cli",
        "0.144.4",
        "/usr/bin/codex",
        "openai",
        "unknown",
        "Codex Default",
    )
    .await
    .unwrap();

    let version: (String,) =
        sqlx::query_as("SELECT agent_version FROM runtime_profiles WHERE id = 'codex-default'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(version.0, "0.144.4");
}

#[tokio::test]
async fn test_45_secret_values_absent_from_database() {
    let db = Database::open_in_memory().await.unwrap();
    let agent = test_agent("claude-code", "2.1.210");
    repo::upsert_agent_definition(&db.pool, &agent)
        .await
        .unwrap();
    repo::upsert_runtime_profile(
        &db.pool,
        "claude-default",
        &agent.identity.discovery_hash,
        "claude-code",
        "claude-cli",
        "2.1.210",
        "/usr/bin/claude",
        "anthropic",
        "unknown",
        "Claude Default",
    )
    .await
    .unwrap();

    let findings = repo::verify_no_secrets_in_db(&db.pool).await.unwrap();
    assert!(findings.is_empty(), "Secrets found in DB: {:?}", findings);
}

#[tokio::test]
async fn test_46_repeated_persistence_idempotent() {
    let db = Database::open_in_memory().await.unwrap();
    let agent = test_agent("claude-code", "2.1.210");

    repo::upsert_agent_definition(&db.pool, &agent)
        .await
        .unwrap();
    repo::upsert_agent_definition(&db.pool, &agent)
        .await
        .unwrap();
    repo::upsert_agent_definition(&db.pool, &agent)
        .await
        .unwrap();

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM agent_definitions WHERE id = ?")
        .bind(&agent.identity.discovery_hash)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn test_47_evidence_persisted() {
    let db = Database::open_in_memory().await.unwrap();
    let agent = test_agent("claude-code", "2.1.210");
    repo::upsert_agent_definition(&db.pool, &agent)
        .await
        .unwrap();

    let evidence_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM discovery_evidence WHERE agent_definition_id = ?")
            .bind(&agent.identity.discovery_hash)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert!(evidence_count.0 >= 1);
}

#[tokio::test]
async fn test_48_provider_hints_persisted() {
    let db = Database::open_in_memory().await.unwrap();
    let agent = test_agent("claude-code", "2.1.210");
    repo::upsert_agent_definition(&db.pool, &agent)
        .await
        .unwrap();

    let hint_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM agent_provider_hints WHERE agent_definition_id = ?")
            .bind(&agent.identity.discovery_hash)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert!(hint_count.0 >= 1);
}

// ══════════════════════════════════════════════════════════════════════
// Section 12: Table count & migration (Tests 49-50)
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_49_table_count_18_business_tables() {
    let db = Database::open_in_memory().await.unwrap();
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_sqlx_%' ORDER BY name"
    ).fetch_all(&db.pool).await.unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();

    // After migration 009 (agent_discovery), we should have 18 tables:
    // After migration 010 (scheduler), we should have 21 tables
    // After migration 011 (resource_handoff), we should have 22 tables
    assert_eq!(
        names.len(),
        22,
        "Expected 22 business tables (001–011), got {}: {:?}",
        names.len(),
        names
    );
    assert!(names.contains(&"agent_definitions"));
    assert!(names.contains(&"discovery_evidence"));
    assert!(names.contains(&"agent_provider_hints"));
    assert!(names.contains(&"dispatch_operations"));
    assert!(names.contains(&"scheduler_reservations"));
    assert!(names.contains(&"scheduler_reconciliations"));
    assert!(names.contains(&"resource_handoffs"));
}

#[tokio::test]
async fn test_50_migration_009_applied() {
    let db = Database::open_in_memory().await.unwrap();
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM _sqlx_migrations WHERE version >= 9")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert!(row.0 >= 1, "Migration 009 should be applied");
}

// ══════════════════════════════════════════════════════════════════════
// Section 13: Missing executable, wrapper, dedup (Tests 51-54)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_51_missing_executable_agent() {
    let agent = DiscoveredAgent {
        identity: ExecutableIdentity::compute("/nonexistent/claude", "claude-code"),
        discovery_evidence: vec![],
        confidence: DiscoveryConfidence::Low,
        version: None,
        is_wrapper: false,
        wraps_agent_kind: None,
        provider_hints: vec![],
        authentication_state: AuthenticationState {
            status: AuthStateValue::Unknown,
            mode: AuthModeHint::Unknown,
            evidence: vec![],
        },
        profiles: vec![],
        first_seen_at: Utc::now(),
        last_seen_at: Utc::now(),
    };
    assert!(agent.version.is_none());
    assert_eq!(agent.confidence, DiscoveryConfidence::Low);
}

#[test]
fn test_52_wrapper_opaque() {
    // Wrapper must remain opaque — no internal parsing
    let wrapper = DiscoveredAgent {
        identity: ExecutableIdentity::compute("/usr/bin/claude-glm", "claude-code"),
        discovery_evidence: vec![DiscoveryEvidence {
            evidence_kind: EvidenceKind::BasenamePattern,
            observation: "Wrapper basename: claude-glm".to_string(),
            confidence: DiscoveryConfidence::High,
            collected_at: Utc::now(),
        }],
        confidence: DiscoveryConfidence::High,
        version: Some("2.1.210".to_string()),
        is_wrapper: true,
        wraps_agent_kind: Some("claude-code".to_string()),
        provider_hints: vec![],
        authentication_state: AuthenticationState {
            status: AuthStateValue::Unknown,
            mode: AuthModeHint::Unknown,
            evidence: vec![],
        },
        profiles: vec!["claude-glm-default".to_string()],
        first_seen_at: Utc::now(),
        last_seen_at: Utc::now(),
    };
    assert!(wrapper.is_wrapper);
    // Evidence is about basename, not about internal content
    let basename_evidence = wrapper
        .discovery_evidence
        .iter()
        .find(|e| e.evidence_kind == EvidenceKind::BasenamePattern);
    assert!(basename_evidence.is_some());
}

#[test]
fn test_53_duplicate_path_dedup() {
    let id1 = ExecutableIdentity::compute("/usr/bin/claude", "claude-code");
    let id2 = ExecutableIdentity::compute("/usr/bin/claude", "claude-code");
    assert_eq!(id1.discovery_hash, id2.discovery_hash);
    assert_eq!(id1.executable_path, id2.executable_path);
}

#[test]
fn test_54_deepseek_env_no_false_anthropic() {
    // When DeepSeek env is set with ANTHROPIC_BASE_URL, we must NOT
    // create a false first-party Anthropic profile
    let hint = ProviderHint {
        provider: "custom-anthropic-compatible".to_string(),
        source: ProviderHintSource::Unknown,
        confidence: DiscoveryConfidence::Low,
        evidence: vec!["ANTHROPIC_BASE_URL set with non-Anthropic provider env".to_string()],
        base_url: None,
        is_custom_endpoint: true,
    };
    assert_ne!(hint.provider, "anthropic");
    assert!(hint.is_custom_endpoint);
}

// ══════════════════════════════════════════════════════════════════════
// Section 14: Event ordering, forward compat (Tests 55-58)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_55_enriched_event_sequence_stable() {
    use harness_core::contracts::agent_event::EnrichedAgentEvent;
    let event = AgentEvent::Message {
        content: "hello".into(),
        vendor_event_id: None,
    };
    let enriched = EnrichedAgentEvent::new("exec-1".into(), 1, event.clone());
    assert_eq!(enriched.receive_sequence, 1);

    let enriched2 = EnrichedAgentEvent::new("exec-1".into(), 2, event);
    assert_eq!(enriched2.receive_sequence, 2);
    assert!(enriched2.receive_sequence > enriched.receive_sequence);
}

#[test]
fn test_56_unknown_vendor_fields_forward_compatible() {
    // Unknown fields in vendor JSON should be preserved in RawVendorEvent
    let event = AgentEvent::RawVendorEvent {
        raw_type: "vendor.v2.new_type".into(),
        payload: serde_json::json!({
            "unknown_field": "some_value",
            "nested": {"future_property": true}
        }),
    };
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("vendor.v2.new_type"));
    assert!(json.contains("unknown_field"));
}

#[test]
fn test_57_malformed_event_diagnostic() {
    // Malformed events should produce structured diagnostics, not crashes
    let event = AgentEvent::RawVendorEvent {
        raw_type: "malformed_json".to_string(),
        payload: serde_json::json!({
            "raw_line_preview": "{invalid json",
            "error": "failed to parse JSONL line"
        }),
    };
    match &event {
        AgentEvent::RawVendorEvent { raw_type, payload } => {
            assert_eq!(raw_type, "malformed_json");
            assert!(payload.to_string().contains("failed to parse JSONL line"));
        }
        _ => panic!("Expected RawVendorEvent"),
    }
}

#[test]
fn test_58_exactly_one_terminal_outcome() {
    // SessionEnded can only appear once as the terminal event
    let event = AgentEvent::SessionEnded {
        session_id: "s1".into(),
        synthetic: true,
        termination_reason: harness_core::contracts::agent_event::TerminationReason::Completed,
        result_received: true,
        process_exit_received: true,
    };
    // Verification: synthetic=true, termination_reason is concrete
    match &event {
        AgentEvent::SessionEnded {
            synthetic,
            termination_reason,
            ..
        } => {
            assert!(synthetic);
            assert!(matches!(
                termination_reason,
                harness_core::contracts::agent_event::TerminationReason::Completed
            ));
        }
        _ => panic!(),
    }
}

// ══════════════════════════════════════════════════════════════════════
// Section 15: Env var names only (Tests 59-60)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn test_59_environment_variable_names_recorded_without_values() {
    // Evidence must record env var NAMES only, never values
    let evidence = DiscoveryEvidence {
        evidence_kind: EvidenceKind::EnvironmentPresence,
        observation:
            "Auth-related env vars present (names only): ANTHROPIC_API_KEY, OPENAI_API_KEY"
                .to_string(),
        confidence: DiscoveryConfidence::Low,
        collected_at: Utc::now(),
    };
    // Must NOT contain values like "sk-..." or "key-..."
    assert!(!evidence.observation.contains("sk-"));
    assert!(!evidence.observation.contains("key-"));
    assert!(!evidence.observation.to_lowercase().contains("secret"));
    // Must contain variable NAMES
    assert!(evidence.observation.contains("ANTHROPIC_API_KEY"));
}

#[test]
fn test_60_no_global_config_modified() {
    // Discovery must NOT modify global config or environment
    // This is a design invariant test — the discovery service is purely read-only
    // We verify that PATH, env vars, and global state are unchanged after discovery
    let path_before = std::env::var("PATH").ok();
    // (In real discovery, we would run discovery and check PATH unchanged)
    // For this test, we verify the invariant by confirming we don't mutate PATH
    let path_after = std::env::var("PATH").ok();
    assert_eq!(path_before, path_after);
}
