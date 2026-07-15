//! AgentDefinition — represents a discovered Agent execution engine
//! (Claude CLI, Codex CLI, etc.), independent of provider/model.

use serde::{Deserialize, Serialize};

/// An Agent execution engine discovered on the system.
/// One AgentDefinition can have multiple RuntimeProfiles
/// (e.g., claude-native, claude-deepseek, claude-glm).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDefinition {
    /// Unique ID: "{agent_kind}-{discovery_hash}"
    pub id: String,
    /// "claude-code" | "codex" | "gemini" | "custom"
    pub agent_kind: String,
    /// Human-readable label
    pub label: String,
    /// Absolute path to the executable
    pub executable_path: String,
    /// How this was discovered
    pub discovery_source: DiscoverySource,
    /// Version string from --version
    pub version: Option<String>,
    /// Whether this is a wrapper script (claude-glm, etc.)
    pub is_wrapper: bool,
    /// If a wrapper, the base agent kind it wraps
    pub wraps_agent_kind: Option<String>,
    /// Passive discovery status
    pub passive_status: PassiveDiscoveryStatus,
    /// Diagnostics from passive discovery
    pub diagnostics: Vec<Diagnostic>,
    /// Runtime profiles derived from this agent
    pub profiles: Vec<String>, // RuntimeProfile IDs
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoverySource {
    /// Found in PATH
    Path { directory: String },
    /// Common install directory
    InstallDir { path: String },
    /// User explicitly registered
    UserRegistered { registered_at: String },
    /// Sidecar profile manifest
    SidecarManifest { manifest_path: String },
    /// Built-in template (not auto-enabled)
    BuiltInTemplate,
    /// Inherited from environment at Harness startup
    Environment { var_name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PassiveDiscoveryStatus {
    /// Executable found, version confirmed
    Detected,
    /// Configuration parseable (no API key read)
    Configured,
    /// Authentication confirmed via Agent's own status command
    Authenticated,
    /// Active validation (paid probe) completed
    Validated,
    /// Passive discovery failed
    Failed { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub level: DiagnosticLevel,
    pub message: String,
    pub source: String, // "version_check" | "auth_check" | "config_parse" | "wrapper_inspection"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticLevel {
    Info,
    Warning,
    Error,
}
