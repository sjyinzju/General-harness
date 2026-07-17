//! Agent Discovery Service — passive discovery of installed Agent CLIs.
//!
//! Passive discovery:
//! - Scans PATH for known executables (claude, codex)
//! - Runs `--version` and `--help` via ProcessManager (no model invocation)
//! - Detects wrapper scripts by basename pattern
//! - Collects environment evidence (variable names only, never values)
//! - Produces DiscoveredAgent with ProviderHints (always with evidence/confidence)
//!
//! Active validation is opt-in and NEVER triggered by passive discovery.
//! Environment variable values are never read, stored, or logged.

pub mod known_agents;
pub mod repo;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use harness_core::contracts::discovery::{
    AuthModeHint, AuthStateValue, AuthenticationState, CapabilityNegotiation, DiscoveredAgent,
    DiscoveryConfidence, DiscoveryEvidence, EvidenceKind, ExecutableIdentity, ProviderHint,
    ProviderHintSource,
};
use harness_core::CoreError;

use crate::process::manager::ProcessManager;
use crate::process::types::{CapturePolicy, ProcessSpec, StdinMode};

/// Max runtime for a version/help probe — these should return near-instantly.
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
/// Max output bytes from --version or --help.
const PROBE_OUTPUT_LIMIT: usize = 64 * 1024; // 64 KiB

/// AgentDiscoveryService performs passive discovery of Agent CLIs.
pub struct AgentDiscoveryService {
    process_manager: Arc<ProcessManager>,
    /// Additional executable paths registered by user via Harness config.
    custom_executables: Vec<CustomExecutable>,
    /// Additional PATH entries to scan beyond OS PATH.
    extra_path_entries: Vec<PathBuf>,
}

/// A user-registered executable to include in discovery.
#[derive(Debug, Clone)]
pub struct CustomExecutable {
    pub path: PathBuf,
    pub agent_kind: Option<String>,
    pub label: Option<String>,
    pub is_wrapper: bool,
    pub wraps_agent_kind: Option<String>,
}

impl AgentDiscoveryService {
    pub fn new(process_manager: Arc<ProcessManager>) -> Self {
        Self {
            process_manager,
            custom_executables: Vec::new(),
            extra_path_entries: Vec::new(),
        }
    }

    /// Register a custom executable for discovery.
    pub fn register_custom(&mut self, exe: CustomExecutable) {
        self.custom_executables.push(exe);
    }

    /// Add an extra directory to scan beyond the OS PATH.
    pub fn add_extra_path(&mut self, path: PathBuf) {
        self.extra_path_entries.push(path);
    }

    /// Run passive discovery: find all known agent executables.
    /// Returns discovered agents. Idempotent — same inputs produce stable identities.
    pub async fn discover(&self) -> Result<Vec<DiscoveredAgent>, CoreError> {
        let mut agents: Vec<DiscoveredAgent> = Vec::new();

        // 1. Scan PATH for known agent basenames
        let path_dirs = self.path_directories();
        let known = known_agents::known_agents();

        for pattern in &known {
            // Check each PATH directory for this basename
            for dir in &path_dirs {
                let exe_path = self.resolve_executable(dir, &pattern.basename);
                if let Some(full_path) = exe_path {
                    // Check for duplicates (same executable path, same agent kind)
                    let identity = ExecutableIdentity::compute(
                        &full_path.to_string_lossy(),
                        &pattern.agent_kind,
                    );
                    if agents.iter().any(|a| {
                        a.identity.discovery_hash == identity.discovery_hash
                            && a.identity.executable_path == identity.executable_path
                    }) {
                        continue;
                    }

                    if let Some(agent) = self.probe_agent(&full_path, pattern, false, None).await {
                        agents.push(agent);
                    }
                }
            }
        }

        // 2. Scan for wrapper basenames
        for dir in &path_dirs {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let basename = entry.file_name().to_string_lossy().to_string();
                    if let Some(wraps_kind) = known_agents::is_wrapper_basename(&basename) {
                        let full_path = entry.path();
                        let identity =
                            ExecutableIdentity::compute(&full_path.to_string_lossy(), &wraps_kind);
                        if agents
                            .iter()
                            .any(|a| a.identity.discovery_hash == identity.discovery_hash)
                        {
                            continue;
                        }
                        // Find the matching known pattern for the wrapped agent kind
                        if let Some(pattern) = known.iter().find(|k| k.agent_kind == wraps_kind) {
                            if let Some(agent) = self
                                .probe_agent(&full_path, pattern, true, Some(wraps_kind.clone()))
                                .await
                            {
                                agents.push(agent);
                            }
                        }
                    }
                }
            }
        }

        // 3. Probe custom executables
        for custom in &self.custom_executables {
            if custom.path.exists() {
                let basename = custom
                    .path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("custom");
                let agent_kind = custom
                    .agent_kind
                    .clone()
                    .unwrap_or_else(|| format!("custom-{}", basename));
                let adapter_kind = "custom-cli".to_string();

                let pattern = known_agents::KnownAgentPattern {
                    basename: basename.to_string(),
                    agent_kind: agent_kind.clone(),
                    adapter_kind,
                    version_flag: "--version".to_string(),
                    help_flag: "--help".to_string(),
                    has_status_command: false,
                    status_args: vec![],
                };

                let identity =
                    ExecutableIdentity::compute(&custom.path.to_string_lossy(), &agent_kind);
                if agents
                    .iter()
                    .any(|a| a.identity.discovery_hash == identity.discovery_hash)
                {
                    continue;
                }

                if let Some(agent) = self
                    .probe_agent(
                        &custom.path,
                        &pattern,
                        custom.is_wrapper,
                        custom.wraps_agent_kind.clone(),
                    )
                    .await
                {
                    agents.push(agent);
                }
            }
        }

        // Sort by agent_kind then executable_path for stable ordering
        agents.sort_by(|a, b| {
            a.identity
                .agent_kind
                .cmp(&b.identity.agent_kind)
                .then_with(|| a.identity.executable_path.cmp(&b.identity.executable_path))
        });

        Ok(agents)
    }

    /// Probe a single executable: get version, parse help, collect evidence.
    async fn probe_agent(
        &self,
        exe_path: &Path,
        pattern: &known_agents::KnownAgentPattern,
        is_wrapper: bool,
        wraps_agent_kind: Option<String>,
    ) -> Option<DiscoveredAgent> {
        let now = Utc::now();
        let identity =
            ExecutableIdentity::compute(&exe_path.to_string_lossy(), &pattern.agent_kind);

        let mut evidence: Vec<DiscoveryEvidence> = Vec::new();

        // Evidence: path resolution
        evidence.push(DiscoveryEvidence {
            evidence_kind: EvidenceKind::PathResolution,
            observation: format!("Executable found at {}", exe_path.display()),
            confidence: DiscoveryConfidence::High,
            collected_at: now,
        });

        // Probe: --version
        let version_result = self.probe_command(exe_path, &[&pattern.version_flag]).await;
        if let Ok(ref output) = version_result {
            let v = output.trim().to_string();
            evidence.push(DiscoveryEvidence {
                evidence_kind: EvidenceKind::VersionOutput,
                observation: format!("Version output: {}", &v[..v.len().min(200)]),
                confidence: DiscoveryConfidence::High,
                collected_at: now,
            });
        }
        let version = version_result.ok().map(|o| o.trim().to_string());

        // Probe: --help
        if let Ok(ref output) = self
            .probe_command(exe_path, &[&pattern.help_flag])
            .await
        {
            evidence.push(DiscoveryEvidence {
                evidence_kind: EvidenceKind::HelpOutput,
                observation: format!("Help output: {} bytes", output.len()),
                confidence: DiscoveryConfidence::Medium,
                collected_at: now,
            });
        }

        // Probe: login status (if safe)
        let auth_state = if pattern.has_status_command {
            self.probe_auth_status(exe_path, pattern, &mut evidence, now)
                .await
        } else {
            self.infer_auth_from_environment(&mut evidence, now)
        };

        // Provider hints
        let provider_hints =
            self.infer_provider_hints(exe_path, &identity, pattern, &version, &mut evidence, now);

        // Basename pattern evidence (for wrappers)
        if is_wrapper {
            evidence.push(DiscoveryEvidence {
                evidence_kind: EvidenceKind::BasenamePattern,
                observation: format!("Wrapper basename: {}", identity.executable_basename),
                confidence: DiscoveryConfidence::High,
                collected_at: now,
            });
        }

        // Build capability negotiation (all unknown until active validation)
        let _capabilities = CapabilityNegotiation::all_unknown();

        // Build profile IDs
        let profiles = vec![format!("{}-default", identity.discovery_hash)];

        Some(DiscoveredAgent {
            identity,
            discovery_evidence: evidence,
            confidence: DiscoveryConfidence::High,
            version,
            is_wrapper,
            wraps_agent_kind,
            provider_hints,
            authentication_state: auth_state,
            profiles,
            first_seen_at: now,
            last_seen_at: now,
        })
    }

    /// Run a safe probing command (--version, --help) via ProcessManager.
    async fn probe_command(&self, exe_path: &Path, args: &[&str]) -> Result<String, ()> {
        let exec_id = format!("discovery-probe-{}", uuid::Uuid::new_v4());
        let spec = ProcessSpec {
            executable: exe_path.to_path_buf(),
            args: args.iter().map(|s| s.to_string()).collect(),
            working_directory: std::env::temp_dir(),
            env_overrides: HashMap::new(),
            env_removals: vec![],
            stdin_mode: StdinMode::Closed,
            timeout: PROBE_TIMEOUT,
            graceful_shutdown_timeout: Duration::from_secs(3),
            stdout_capture: CapturePolicy::Spool {
                max_memory_bytes: 4096,
            },
            stderr_capture: CapturePolicy::Spool {
                max_memory_bytes: 4096,
            },
            output_byte_limit: PROBE_OUTPUT_LIMIT,
            spool_dir: None, // no spool needed for short probes
            known_secrets: vec![],
            execution_id: exec_id.clone(),
            runtime_profile_id: String::new(),
        };

        let _handle = self.process_manager.spawn(&spec).await.map_err(|_| ())?;

        // Wait for process to complete (poll state)
        let mut waited = 0;
        loop {
            let state = self.process_manager.get_state(&exec_id).await;
            match state {
                Some(crate::process::types::ProcessState::Completed { outcome }) => {
                    return outcome.stdout_preview.ok_or(()).map(|s| s.to_string());
                }
                Some(crate::process::types::ProcessState::Running) => {
                    if waited > PROBE_TIMEOUT.as_millis() as u64 {
                        let _ = self.process_manager.cancel(&exec_id).await;
                        return Err(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    waited += 100;
                }
                _ => return Err(()),
            }
        }
    }

    /// Probe auth status: run login status command (safe — no secrets read).
    async fn probe_auth_status(
        &self,
        exe_path: &Path,
        pattern: &known_agents::KnownAgentPattern,
        evidence: &mut Vec<DiscoveryEvidence>,
        now: chrono::DateTime<Utc>,
    ) -> AuthenticationState {
        let status_output = self
            .probe_command(
                exe_path,
                &pattern
                    .status_args
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )
            .await;

        match status_output {
            Ok(output) => {
                let lower = output.to_lowercase();
                let is_authenticated = lower.contains("logged in")
                    || lower.contains("authenticated")
                    || lower.contains("active");
                evidence.push(DiscoveryEvidence {
                    evidence_kind: EvidenceKind::AuthStatusCommand,
                    observation: "Auth status command returned successfully".to_string(),
                    confidence: if is_authenticated {
                        DiscoveryConfidence::High
                    } else {
                        DiscoveryConfidence::Medium
                    },
                    collected_at: now,
                });
                AuthenticationState {
                    status: if is_authenticated {
                        AuthStateValue::Authenticated
                    } else {
                        AuthStateValue::Unauthenticated
                    },
                    mode: AuthModeHint::Login,
                    evidence: vec!["login status command confirmed".to_string()],
                }
            }
            Err(_) => self.infer_auth_from_environment(evidence, now),
        }
    }

    /// Infer auth state from environment variable presence (names only).
    fn infer_auth_from_environment(
        &self,
        evidence: &mut Vec<DiscoveryEvidence>,
        now: chrono::DateTime<Utc>,
    ) -> AuthenticationState {
        let mut auth_mode = AuthModeHint::Unknown;
        let mut auth_status = AuthStateValue::Unknown;
        let mut env_evidence: Vec<String> = Vec::new();
        let mut found_env_vars: Vec<String> = Vec::new();

        // Check for known auth-related env var names (names only, never values)
        for (var_name, mode_hint) in env_auth_indicators() {
            if std::env::var(&var_name).is_ok() {
                found_env_vars.push(var_name.clone());
                auth_mode = mode_hint;
                auth_status = AuthStateValue::Unknown; // presence ≠ valid auth
                env_evidence.push(format!(
                    "Environment variable {} is set (value not read)",
                    var_name
                ));
            }
        }

        if !found_env_vars.is_empty() {
            evidence.push(DiscoveryEvidence {
                evidence_kind: EvidenceKind::EnvironmentPresence,
                observation: format!(
                    "Auth-related env vars present (names only): {}",
                    found_env_vars.join(", ")
                ),
                confidence: DiscoveryConfidence::Low,
                collected_at: now,
            });
        }

        AuthenticationState {
            status: auth_status,
            mode: auth_mode,
            evidence: env_evidence,
        }
    }

    /// Infer provider hints from executable identity and environment evidence.
    fn infer_provider_hints(
        &self,
        _exe_path: &Path,
        _identity: &ExecutableIdentity,
        pattern: &known_agents::KnownAgentPattern,
        _version: &Option<String>,
        evidence: &mut Vec<DiscoveryEvidence>,
        now: chrono::DateTime<Utc>,
    ) -> Vec<ProviderHint> {
        let mut hints: Vec<ProviderHint> = Vec::new();

        // Default provider hint based on agent kind
        let default_provider = match pattern.agent_kind.as_str() {
            "claude-code" => "anthropic",
            "codex" => "openai",
            _ => "unknown",
        };

        // Check for provider-overriding environment variable names (names only)
        let mut env_hint_provider: Option<String> = None;
        let mut has_anthropic_base_url = false;

        for (var_name, provider) in env_provider_indicators() {
            if std::env::var(&var_name).is_ok() {
                if var_name == "ANTHROPIC_BASE_URL" {
                    has_anthropic_base_url = true;
                    // ANTHROPIC_BASE_URL alone does NOT make it Anthropic —
                    // it's a custom endpoint hint. Only record the presence.
                    evidence.push(DiscoveryEvidence {
                        evidence_kind: EvidenceKind::EnvironmentPresence,
                        observation:
                            "ANTHROPIC_BASE_URL is set — custom endpoint (value not read)"
                                .to_string(),
                        confidence: DiscoveryConfidence::Low,
                        collected_at: now,
                    });
                } else {
                    env_hint_provider = Some(provider.to_string());
                    evidence.push(DiscoveryEvidence {
                        evidence_kind: EvidenceKind::EnvironmentPresence,
                        observation: format!(
                            "Provider-hinting env var {} is set (value not read)",
                            var_name
                        ),
                        confidence: DiscoveryConfidence::Low,
                        collected_at: now,
                    });
                }
            }
        }

        // Build primary provider hint
        if let Some(ref env_provider) = env_hint_provider {
            // Environment suggests a different provider than default
            hints.push(ProviderHint {
                provider: env_provider.clone(),
                source: ProviderHintSource::EnvironmentHint,
                confidence: DiscoveryConfidence::Low,
                evidence: vec![format!(
                    "Environment variable suggests {} provider",
                    env_provider
                )],
                base_url: None,
                is_custom_endpoint: has_anthropic_base_url,
            });
        }

        // Default provider hint (lower confidence if env overrides exist)
        let default_confidence = if env_hint_provider.is_some() {
            DiscoveryConfidence::Low
        } else {
            DiscoveryConfidence::Medium
        };

        // CRITICAL: If ANTHROPIC_BASE_URL is set and env suggests DeepSeek,
        // we must NOT create a false first-party Anthropic profile.
        // The provider hint for the default must reflect uncertainty.
        if has_anthropic_base_url && env_hint_provider.is_some() {
            // Custom endpoint with non-Anthropic env — don't claim anthropic
            hints.push(ProviderHint {
                provider: "custom-anthropic-compatible".to_string(),
                source: ProviderHintSource::Unknown,
                confidence: DiscoveryConfidence::Low,
                evidence: vec![
                    "ANTHROPIC_BASE_URL set with non-Anthropic provider env".to_string(),
                ],
                base_url: None,
                is_custom_endpoint: true,
            });
        } else if has_anthropic_base_url {
            // ANTHROPIC_BASE_URL with no other provider hint — could be proxy
            hints.push(ProviderHint {
                provider: default_provider.to_string(),
                source: ProviderHintSource::Unknown,
                confidence: DiscoveryConfidence::Low,
                evidence: vec![
                    "ANTHROPIC_BASE_URL set — may be proxy or custom endpoint".to_string()
                ],
                base_url: None,
                is_custom_endpoint: true,
            });
        } else {
            // Standard default provider hint
            hints.push(ProviderHint {
                provider: default_provider.to_string(),
                source: ProviderHintSource::Unknown,
                confidence: default_confidence,
                evidence: vec![format!(
                    "Default provider for {} agents",
                    pattern.agent_kind
                )],
                base_url: None,
                is_custom_endpoint: false,
            });
        }

        hints
    }

    /// Get all directories to scan for executables.
    fn path_directories(&self) -> Vec<PathBuf> {
        let mut dirs: Vec<PathBuf> = Vec::new();

        // OS PATH
        if let Ok(path_var) = std::env::var("PATH") {
            for entry in std::env::split_paths(&path_var) {
                dirs.push(entry);
            }
        }

        // Extra entries
        for extra in &self.extra_path_entries {
            if !dirs.contains(extra) {
                dirs.push(extra.clone());
            }
        }

        dirs
    }

    /// Resolve an executable basename in a directory. Handles Windows extensions.
    fn resolve_executable(&self, dir: &Path, basename: &str) -> Option<PathBuf> {
        let candidates = if cfg!(windows) {
            vec![
                dir.join(format!("{}.cmd", basename)),
                dir.join(format!("{}.ps1", basename)),
                dir.join(format!("{}.bat", basename)),
                dir.join(format!("{}.exe", basename)),
                dir.join(basename),
            ]
        } else {
            vec![dir.join(basename)]
        };

        candidates.into_iter().find(|c| c.is_file())
    }
}

// ── Environment variable name indicators ──────────────────────────────────

/// Auth-related environment variable names → auth mode hint.
/// Names only — values are NEVER read.
fn env_auth_indicators() -> Vec<(String, AuthModeHint)> {
    vec![
        ("ANTHROPIC_API_KEY".to_string(), AuthModeHint::ApiKeyEnv),
        ("OPENAI_API_KEY".to_string(), AuthModeHint::ApiKeyEnv),
        ("DEEPSEEK_API_KEY".to_string(), AuthModeHint::ApiKeyEnv),
    ]
}

/// Provider-indicating environment variable names → provider.
/// Names only — values are NEVER read.
fn env_provider_indicators() -> Vec<(String, String)> {
    vec![
        (
            "ANTHROPIC_BASE_URL".to_string(),
            "anthropic-compatible".to_string(),
        ),
        ("DEEPSEEK_API_KEY".to_string(), "deepseek".to_string()),
        ("OPENAI_API_KEY".to_string(), "openai".to_string()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_core::contracts::discovery::CapabilitySupport;

    #[test]
    fn test_executable_identity_stable() {
        let id1 = ExecutableIdentity::compute("/usr/bin/claude", "claude-code");
        let id2 = ExecutableIdentity::compute("/usr/bin/claude", "claude-code");
        assert_eq!(id1.discovery_hash, id2.discovery_hash);
        assert_eq!(id1.agent_kind, "claude-code");
    }

    #[test]
    fn test_executable_identity_different_paths() {
        let id1 = ExecutableIdentity::compute("/usr/bin/claude", "claude-code");
        let id2 = ExecutableIdentity::compute("/usr/local/bin/claude", "claude-code");
        assert_ne!(id1.discovery_hash, id2.discovery_hash);
    }

    #[test]
    fn test_executable_identity_different_kinds() {
        let id1 = ExecutableIdentity::compute("/usr/bin/claude", "claude-code");
        let id2 = ExecutableIdentity::compute("/usr/bin/claude", "codex");
        assert_ne!(id1.discovery_hash, id2.discovery_hash);
    }

    #[test]
    fn test_wrapper_basename_detected() {
        assert_eq!(
            known_agents::is_wrapper_basename("claude-glm"),
            Some("claude-code".to_string())
        );
        assert_eq!(
            known_agents::is_wrapper_basename("claude-deepseek"),
            Some("claude-code".to_string())
        );
    }

    #[test]
    fn test_non_wrapper_basename_not_detected() {
        assert_eq!(known_agents::is_wrapper_basename("claude"), None);
        assert_eq!(known_agents::is_wrapper_basename("codex"), None);
    }

    #[test]
    fn test_capability_negotiation_all_unknown() {
        let caps = CapabilityNegotiation::all_unknown();
        assert_eq!(caps.execute, CapabilitySupport::Unknown);
        assert_eq!(caps.timeout, CapabilitySupport::Unknown);
        assert_eq!(caps.cancellation, CapabilitySupport::Unknown);
    }
}
