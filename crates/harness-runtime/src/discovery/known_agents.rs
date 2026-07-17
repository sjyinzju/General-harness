//! Known Agent executable patterns for passive discovery.
//!
//! Maps basenames to agent kinds and adapter kinds. Wrapper patterns
//! are matched separately — a wrapper like `claude-glm` is identified
//! as Claude CLI wrapping a different provider, NOT as a separate agent.

/// Description of a known Agent CLI executable.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct KnownAgentPattern {
    /// Primary basename (e.g., "claude", "codex").
    pub basename: String,
    /// Agent kind ("claude-code", "codex").
    pub agent_kind: String,
    /// Default adapter kind.
    pub adapter_kind: String,
    /// Known --version flag.
    pub version_flag: String,
    /// Known --help flag.
    pub help_flag: String,
    /// Whether this agent has a login/status command safe for passive probing.
    pub has_status_command: bool,
    /// Status command args (safe — no model invocation).
    pub status_args: Vec<String>,
}

/// Known agent patterns registered for passive discovery.
pub(crate) fn known_agents() -> Vec<KnownAgentPattern> {
    vec![
        KnownAgentPattern {
            basename: "claude".to_string(),
            agent_kind: "claude-code".to_string(),
            adapter_kind: "claude-cli".to_string(),
            version_flag: "--version".to_string(),
            help_flag: "--help".to_string(),
            has_status_command: false,
            status_args: vec![],
        },
        KnownAgentPattern {
            basename: "codex".to_string(),
            agent_kind: "codex".to_string(),
            adapter_kind: "codex-cli".to_string(),
            version_flag: "--version".to_string(),
            help_flag: "exec".to_string(), // codex exec --help
            has_status_command: true,
            status_args: vec!["login".to_string(), "status".to_string()],
        },
    ]
}

/// Wrapper basename patterns: (pattern, wraps_agent_kind).
/// A basename matching one of these patterns is treated as a wrapper
/// of the specified agent kind, not as a separate agent definition.
pub(crate) fn wrapper_patterns() -> Vec<(String, String)> {
    vec![
        // claude-* wrappers (claude-glm, claude-deepseek, etc.)
        ("claude-".to_string(), "claude-code".to_string()),
    ]
}

/// Check if a basename matches any wrapper pattern.
/// Returns Some(wraps_agent_kind) if it's a wrapper.
pub(crate) fn is_wrapper_basename(basename: &str) -> Option<String> {
    for (pattern, wraps_kind) in wrapper_patterns() {
        if basename.to_lowercase().starts_with(&pattern)
            && basename != pattern.trim_end_matches('-')
        {
            return Some(wraps_kind);
        }
    }
    None
}
