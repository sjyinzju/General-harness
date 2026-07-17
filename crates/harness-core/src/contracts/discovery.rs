//! Agent Discovery types — DiscoveredAgent, evidence, capabilities, profiles.
//!
//! These types model the passive discovery process: PATH scanning, executable
//! identity, version/help probing, wrapper detection, and capability negotiation.
//! Provider information is always a hint with evidence and confidence — never
//! asserted from model name or executable name alone.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── DiscoveredAgent ──────────────────────────────────────────────────────

/// The result of discovering an Agent executable on the system.
/// One discovered agent may yield multiple RuntimeProfiles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredAgent {
    /// Stable identity key derived from executable path + kind.
    pub identity: ExecutableIdentity,
    /// How this agent was found.
    pub discovery_evidence: Vec<DiscoveryEvidence>,
    /// Overall confidence in this discovery.
    pub confidence: DiscoveryConfidence,
    /// Version string (from --version).
    pub version: Option<String>,
    /// Whether this is a wrapper (claude-glm, etc.).
    pub is_wrapper: bool,
    /// If wrapper, what it wraps.
    pub wraps_agent_kind: Option<String>,
    /// Provider hints collected during discovery.
    pub provider_hints: Vec<ProviderHint>,
    /// Auth state (from local login/status command, not from reading secrets).
    pub authentication_state: AuthenticationState,
    /// Profiles derived from this agent.
    pub profiles: Vec<String>,
    /// When this agent was first seen.
    pub first_seen_at: DateTime<Utc>,
    /// When this agent was last seen.
    pub last_seen_at: DateTime<Utc>,
}

// ── ExecutableIdentity ───────────────────────────────────────────────────

/// Stable identity for an Agent executable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutableIdentity {
    /// Canonical absolute path to the executable.
    pub executable_path: String,
    /// Basename of the executable (e.g. "claude", "codex", "claude-glm").
    pub executable_basename: String,
    /// Agent kind: "claude-code" | "codex" | "custom".
    pub agent_kind: String,
    /// Discovery hash for stable identity across restarts.
    pub discovery_hash: String,
}

impl ExecutableIdentity {
    pub fn compute(exe_path: &str, agent_kind: &str) -> Self {
        use sha2::{Digest, Sha256};
        let basename = std::path::Path::new(exe_path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(exe_path)
            .to_string();
        let mut h = Sha256::new();
        h.update(exe_path.as_bytes());
        h.update(agent_kind.as_bytes());
        let hash = hex::encode(h.finalize());
        let short = &hash[..12];
        Self {
            executable_path: exe_path.to_string(),
            executable_basename: basename,
            agent_kind: agent_kind.to_string(),
            discovery_hash: format!("{}-{}", agent_kind, short),
        }
    }
}

// ── DiscoveryEvidence ────────────────────────────────────────────────────

/// A single piece of evidence collected during passive discovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryEvidence {
    /// What kind of evidence.
    pub evidence_kind: EvidenceKind,
    /// What was observed (redacted — no secrets).
    pub observation: String,
    /// Confidence in this piece of evidence.
    pub confidence: DiscoveryConfidence,
    /// When this evidence was collected.
    pub collected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    /// Executable found at a PATH entry.
    PathResolution,
    /// --version output parsed.
    VersionOutput,
    /// --help output parsed for capability flags.
    HelpOutput,
    /// Login/status command output (no secrets read).
    AuthStatusCommand,
    /// Environment variable name observed (name only, no value).
    EnvironmentPresence,
    /// Harness explicit configuration.
    HarnessConfig,
    /// User-provided metadata.
    UserMetadata,
    /// Executable file metadata (size, timestamp, permissions).
    FileMetadata,
    /// Basename pattern match (e.g., "claude-glm" → wrapper).
    BasenamePattern,
}

// ── DiscoveryConfidence ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryConfidence {
    /// Confirmed by multiple independent sources.
    High,
    /// Single source, consistent.
    Medium,
    /// Single source, ambiguous.
    Low,
    /// Hueristic / inferred.
    Heuristic,
}

// ── ProviderHint ─────────────────────────────────────────────────────────

/// Provider information is ALWAYS a hint with evidence and confidence.
/// Never asserted from model name or executable name alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHint {
    /// Provider name (e.g., "anthropic", "deepseek", "openai", "zhipu").
    pub provider: String,
    /// How this hint was derived.
    pub source: ProviderHintSource,
    /// Confidence in this hint.
    pub confidence: DiscoveryConfidence,
    /// Evidence supporting this hint.
    pub evidence: Vec<String>,
    /// Base URL if known from observation (not from reading config secrets).
    pub base_url: Option<String>,
    /// Whether this is definitely a custom/anthropic-compatible endpoint.
    pub is_custom_endpoint: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHintSource {
    /// User explicitly declared.
    UserDeclared,
    /// Matched known endpoint pattern in --help or version output.
    KnownEndpoint,
    /// From sidecar manifest.
    SidecarManifest,
    /// From active probe metadata.
    ProbeMetadata,
    /// Environment variable name suggests provider.
    EnvironmentHint,
    /// Could not determine — fallback.
    Unknown,
}

// ── AuthenticationState ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AuthenticationState {
    /// Current auth status.
    pub status: AuthStateValue,
    /// Auth mode inferred from environment/config presence.
    pub mode: AuthModeHint,
    /// Evidence for this state (never contains values).
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthStateValue {
    Authenticated,
    Unauthenticated,
    Expired,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthModeHint {
    Login,
    ApiKeyEnv,
    Keychain,
    OAuth,
    None,
    Unknown,
}

// ── CapabilitySupport ────────────────────────────────────────────────────

/// How a capability is supported.
/// Distinct from TriState (Supported/Unsupported/Unknown) — this captures
/// whether the support is native or harness-emulated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySupport {
    /// Agent provides this natively.
    Native,
    /// Harness provides this via ProcessManager/other infrastructure.
    HarnessEmulated,
    /// Confirmed unsupported.
    Unsupported,
    /// Not yet verified.
    Unknown,
}

/// Capability negotiation result for each capability dimension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityNegotiation {
    pub execute: CapabilitySupport,
    pub working_directory: CapabilitySupport,
    pub stream_output: CapabilitySupport,
    pub final_result: CapabilitySupport,
    pub process_exit: CapabilitySupport,
    pub timeout: CapabilitySupport,
    pub cancellation: CapabilitySupport,
    pub structured_events: CapabilitySupport,
    pub native_resume: CapabilitySupport,
    pub file_attachments: CapabilitySupport,
}

impl CapabilityNegotiation {
    /// Default: all unknown until verified.
    pub fn all_unknown() -> Self {
        Self {
            execute: CapabilitySupport::Unknown,
            working_directory: CapabilitySupport::Unknown,
            stream_output: CapabilitySupport::Unknown,
            final_result: CapabilitySupport::Unknown,
            process_exit: CapabilitySupport::Unknown,
            timeout: CapabilitySupport::Unknown,
            cancellation: CapabilitySupport::Unknown,
            structured_events: CapabilitySupport::Unknown,
            native_resume: CapabilitySupport::Unknown,
            file_attachments: CapabilitySupport::Unknown,
        }
    }
}

// ── AdapterCompatibility ─────────────────────────────────────────────────

/// Compatibility diagnostic between an Agent executable and an Adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterCompatibility {
    /// Whether the adapter can drive this agent.
    pub compatible: bool,
    /// Adapter kind that checked compatibility.
    pub adapter_kind: String,
    /// Agent version that was checked.
    pub agent_version: String,
    /// Structured diagnostics.
    pub diagnostics: Vec<CompatibilityDiagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityDiagnostic {
    pub level: DiagnosticLevel,
    pub category: String,
    pub message: String,
    pub suggestion: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticLevel {
    Info,
    Warning,
    Error,
    /// Fatal — cannot drive this agent version.
    Fatal,
}

// ── ValidationStatus ─────────────────────────────────────────────────────

/// Status of active validation (opt-in, requires explicit user permission).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationStatus {
    /// Whether validation has been performed.
    pub validated: bool,
    /// When validation was last performed.
    pub validated_at: Option<DateTime<Utc>>,
    /// Validation result summary.
    pub result: Option<ValidationResult>,
    /// Command fingerprint used for validation.
    pub command_fingerprint: Option<String>,
    /// Exit status from validation.
    pub exit_status: Option<i32>,
    /// Structured diagnostics from validation.
    pub diagnostics: Vec<CompatibilityDiagnostic>,
    /// Reference to validation artifact (redacted output).
    pub artifact_reference: Option<String>,
    /// Whether validation requires payment/API cost.
    pub may_incur_cost: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationResult {
    Passed,
    Failed,
    Degraded,
    TimedOut,
    Cancelled,
}

// ── ActiveValidationRequest (what must be shown to user before running) ──

/// Information that must be displayed to the user before running active
/// validation. Never auto-triggered by passive discovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveValidationRequest {
    pub executable: String,
    pub full_args: Vec<String>,
    pub profile_id: String,
    pub working_directory: String,
    pub timeout_secs: u64,
    pub may_incur_cost: bool,
    pub env_var_names: Vec<String>,
}
