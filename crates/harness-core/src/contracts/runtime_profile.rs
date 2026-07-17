//! RuntimeProfile — a specific runnable combination of Agent executable,
//! provider, model policy, authentication source, environment, and capabilities.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A runnable profile: Agent + Provider + Model + Auth + Capabilities.
/// Multiple RuntimeProfiles can reference the same AgentDefinition
/// (e.g., claude-native, claude-deepseek, claude-glm).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProfile {
    pub id: String,
    pub agent_definition_id: String,
    pub label: String,

    // ── Agent ────────────────────────────────────
    pub agent_kind: String,
    pub adapter_kind: String,
    pub agent_version: String,
    pub executable_path: String,

    // ── Provider ─────────────────────────────────
    pub provider: String,
    pub provider_source: ProviderSource,
    pub model: Option<String>,
    pub base_url: Option<String>,

    // ── Auth ─────────────────────────────────────
    pub auth_mode: AuthMode,
    pub auth_status: AuthStatus,
    /// Reference only — never the value
    pub credential_ref: Option<CredentialReference>,

    // ── Capabilities ─────────────────────────────
    pub capabilities: CapabilitySet,

    // ── Status ───────────────────────────────────
    pub core_status: CoreStatus,
    pub authentication_status: AuthCheckStatus,
    pub execution_status: ExecutionStatus,
    pub optional_integrations: Vec<OptionalIntegration>,

    // ── Discovery ────────────────────────────────
    pub discovery_source: String,
    pub passive_probe: Option<PassiveProbeResult>,
    pub active_validation: Option<ActiveValidationResult>,

    // ── Scheduling ───────────────────────────────
    pub concurrency_max: u32,

    // ── Metadata ─────────────────────────────────
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// How the provider was identified.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderSource {
    /// User explicitly declared
    UserDeclared,
    /// Matched known endpoint
    KnownEndpoint,
    /// From sidecar manifest
    SidecarManifest,
    /// From active probe metadata
    ProbeMetadata,
    /// Could not identify — marked as custom
    CustomAnthropicCompatible,
    CustomOpenAiCompatible,
    CustomUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    Login,
    ApiKeyEnv,
    Keychain,
    OAuth,
    None,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthStatus {
    Authenticated,
    Unauthenticated,
    Expired,
    Unknown,
}

/// Credential reference — never contains the actual key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialReference {
    pub source_type: String,  // "env_var" | "keychain" | "login_session"
    pub source_label: String, // "ANTHROPIC_API_KEY" | "com.anthropic.claude-keychain"
}

// ── Status dimensions ─────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoreStatus {
    Available,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthCheckStatus {
    Authenticated,
    Unauthenticated,
    Expired,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Untested,
    SmokeTestPassed,
    SmokeTestFailed { reason: String },
    Degraded { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionalIntegration {
    pub name: String,
    pub status: IntegrationStatus,
    pub required: bool,
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationStatus {
    Supported,
    Unsupported,
    Unknown,
    DegradedStartupTimeout,
    DegradedAuthError,
}

// ── Capability ────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilitySet {
    pub required: RequiredCapabilities,
    pub optional: OptionalCapabilities,
    pub workspace_modes: Vec<String>,
    pub supported_languages: Vec<String>,
    pub mcp_tools: Vec<String>,
    pub supported_platforms: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequiredCapabilities {
    pub execute: TriState,
    pub working_directory: TriState,
    pub stream_output: TriState,
    pub process_exit: TriState,
    pub cancellation: TriState,
    pub timeout: TriState,
    pub final_result: TriState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionalCapabilities {
    pub native_session_resume: TriState,
    pub structured_output: TriState,
    pub tool_events: TriState,
    pub file_change_events: TriState,
    pub reasoning_summary: TriState,
    pub interactive_approval: TriState,
    pub usage_reporting: TriState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriState {
    Supported,
    Unsupported,
    Unknown,
}

// ── Probe ─────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PassiveProbeResult {
    pub executable_found: bool,
    pub version_checked: bool,
    pub config_parseable: bool,
    pub auth_check_passed: bool,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveValidationResult {
    pub validated_at: DateTime<Utc>,
    pub smoke_test_passed: bool,
    pub checks: ActiveProbeChecks,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveProbeChecks {
    pub execute: bool,
    pub stream_output: bool,
    pub final_result: bool,
    pub cancellation: bool,
    pub exit_code_correct: bool,
}

// Legacy alias for backward compat
pub use ActiveProbeChecks as ProbeChecks;
pub type ProbeResult = ActiveValidationResult;
pub type RuntimeProfileStatus = CoreStatus;
