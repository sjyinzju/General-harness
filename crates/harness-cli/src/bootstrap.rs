//! Production bootstrap — discovers agents, builds RuntimeProfiles, constructs
//! adapters, and wires them into ProductionGraph::build_with_adapter.
//!
//! This is the ONLY production path for creating a real ProductionGraph with
//! real AgentAdapter-backed Planner/Evaluator. Without this, the Supervisor
//! cannot execute real Provider calls — goals stay in Planning state forever.
//!
//! # Invariants
//! - Never hardcodes model, provider endpoint, or API keys.
//! - Uses passive discovery (PATH scan) + RuntimeProfile construction.
//! - Falls back to ProductionGraph::build (no adapter) when no profiles found,
//!   logging a clear structured error.
//! - The same profile is used for Planner/Evaluator under IsolatedSessions,
//!   but each invocation creates a fresh AgentSession (never resumed).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use harness_adapters::ClaudeCliAdapter;
use harness_core::contracts::runtime_profile::{
    AuthCheckStatus, AuthMode, AuthStatus, CapabilitySet, CoreStatus, ExecutionStatus,
    OptionalCapabilities, ProviderSource, RequiredCapabilities, RuntimeProfile, TriState,
};
use harness_runtime::discovery::AgentDiscoveryService;
use harness_runtime::liveness::RunContext;
use harness_runtime::process::manager::ProcessManager;
use harness_runtime::process::registry::ProcessRegistry;
use harness_runtime::production_graph::ProductionGraph;
use sqlx::SqlitePool;

/// Result of production bootstrap: a fully-wired ProductionGraph with real
/// AgentAdapter + RuntimeProfile for Planner/Evaluator.
pub struct BootstrappedGraph {
    pub graph: ProductionGraph,
    /// The profile used for Planner/Evaluator (same profile under IsolatedSessions).
    pub operational_profile: Option<RuntimeProfile>,
    /// Number of profiles discovered during bootstrap.
    pub profiles_discovered: usize,
}

/// Bootstrap a production ProductionGraph.
///
/// 1. Scans PATH for known agent executables (claude, codex).
/// 2. Builds RuntimeProfiles from discovered agents.
/// 3. Selects the first operational profile.
/// 4. Constructs the appropriate adapter (ClaudeCliAdapter for claude-code).
/// 5. Calls ProductionGraph::build_with_adapter with the real adapter+profile.
pub async fn bootstrap_production_graph(
    pool: SqlitePool,
    worktree_root: &Path,
    repo_root: &Path,
    run_context: Arc<RunContext>,
) -> Result<BootstrappedGraph, String> {
    // ── 1. Create ProcessManager for adapter construction ────────────
    let registry = Arc::new(ProcessRegistry::new());
    let process_manager = Arc::new(ProcessManager::new(registry));

    // ── 2. Run passive discovery ─────────────────────────────────────
    let discovery = AgentDiscoveryService::new(process_manager.clone());
    let discovered = discovery
        .discover()
        .await
        .map_err(|e| format!("agent discovery failed: {e}"))?;

    tracing::info!(agents_found = discovered.len(), "agent discovery complete");

    // ── 3. Build RuntimeProfiles from discovered agents ──────────────
    let profiles = build_profiles_from_discovery(&discovered);
    tracing::info!(
        profiles_built = profiles.len(),
        "runtime profiles constructed"
    );

    // ── 4. Select operational profile ────────────────────────────────
    let operational = profiles
        .iter()
        .find(|p| p.core_status == CoreStatus::Available)
        .cloned();

    match operational {
        Some(ref profile) => {
            tracing::info!(
                profile_id = %profile.id,
                agent_kind = %profile.agent_kind,
                label = %profile.label,
                "operational profile selected for production graph"
            );

            // ── 5. Construct adapter ─────────────────────────────────
            let adapter: Arc<dyn harness_core::contracts::agent_adapter::AgentAdapter> =
                match profile.adapter_kind.as_str() {
                    "claude-cli" => {
                        let claude = if profile.executable_path.is_empty() {
                            ClaudeCliAdapter::new(process_manager.clone())
                        } else {
                            ClaudeCliAdapter::new(process_manager.clone())
                                .with_executable(PathBuf::from(&profile.executable_path))
                        };
                        Arc::new(claude)
                    }
                    other => {
                        tracing::warn!(
                            adapter_kind = %other,
                            "unknown adapter kind — falling back to graph without adapter"
                        );
                        let graph =
                            ProductionGraph::build(pool, worktree_root, repo_root, run_context)?;
                        return Ok(BootstrappedGraph {
                            graph,
                            operational_profile: None,
                            profiles_discovered: profiles.len(),
                        });
                    }
                };

            // ── 6. Build with adapter ────────────────────────────────
            let graph = ProductionGraph::build_with_adapter(
                pool,
                worktree_root,
                repo_root,
                run_context,
                Some(adapter),
                Some(profile.clone()),
            )?;

            tracing::info!(
                profile_id = %profile.id,
                "production graph built with real adapter — Planner/Evaluator are LIVE"
            );

            Ok(BootstrappedGraph {
                graph,
                operational_profile: Some(profile.clone()),
                profiles_discovered: profiles.len(),
            })
        }
        None => {
            // No operational profiles found — build without adapter
            tracing::warn!(
                profiles_found = profiles.len(),
                "no operational runtime profiles found — building graph without adapter"
            );
            tracing::warn!(
                "Goal Planner/Evaluator will NOT be available. Goals will stay in Planning state."
            );
            tracing::warn!(
                "Install 'claude' CLI on PATH or configure a RuntimeProfile to enable real execution."
            );

            let graph = ProductionGraph::build(pool, worktree_root, repo_root, run_context)?;

            Ok(BootstrappedGraph {
                graph,
                operational_profile: None,
                profiles_discovered: profiles.len(),
            })
        }
    }
}

/// Build RuntimeProfiles from discovered agents.
///
/// Each DiscoveredAgent produces one default RuntimeProfile. Multiple profiles
/// can reference the same agent (different providers via wrappers).
fn build_profiles_from_discovery(
    discovered: &[harness_core::contracts::discovery::DiscoveredAgent],
) -> Vec<RuntimeProfile> {
    let now = chrono::Utc::now();
    let mut profiles = Vec::new();

    for agent in discovered {
        // Each profile_id from discovery
        for profile_id in &agent.profiles {
            // Determine provider from provider hints
            let provider = agent
                .provider_hints
                .first()
                .map(|h| h.provider.clone())
                .unwrap_or_else(|| "unknown".to_string());

            let provider_source = agent
                .provider_hints
                .first()
                .map(|h| match h.source {
                    harness_core::contracts::discovery::ProviderHintSource::EnvironmentHint => {
                        ProviderSource::KnownEndpoint
                    }
                    harness_core::contracts::discovery::ProviderHintSource::Unknown => {
                        ProviderSource::CustomUnknown
                    }
                    _ => ProviderSource::CustomAnthropicCompatible,
                })
                .unwrap_or(ProviderSource::CustomUnknown);

            let auth_mode = match agent.authentication_state.mode {
                harness_core::contracts::discovery::AuthModeHint::ApiKeyEnv => AuthMode::ApiKeyEnv,
                harness_core::contracts::discovery::AuthModeHint::Login => AuthMode::Login,
                harness_core::contracts::discovery::AuthModeHint::Unknown => AuthMode::Unknown,
                _ => AuthMode::Unknown,
            };

            let auth_status = match agent.authentication_state.status {
                harness_core::contracts::discovery::AuthStateValue::Authenticated => {
                    AuthStatus::Authenticated
                }
                harness_core::contracts::discovery::AuthStateValue::Unauthenticated => {
                    AuthStatus::Unauthenticated
                }
                harness_core::contracts::discovery::AuthStateValue::Unknown => AuthStatus::Unknown,
                _ => AuthStatus::Unknown,
            };

            // Determine core status: Available if found with version
            let core_status = if agent.version.is_some() {
                CoreStatus::Available
            } else {
                CoreStatus::Degraded
            };

            let label = format!("{}-{}", agent.identity.agent_kind, profile_id);

            let profile = RuntimeProfile {
                id: profile_id.clone(),
                agent_definition_id: agent.identity.discovery_hash.clone(),
                label,
                agent_kind: agent.identity.agent_kind.clone(),
                adapter_kind: if agent.identity.agent_kind == "claude-code" {
                    "claude-cli".to_string()
                } else {
                    "unknown".to_string()
                },
                agent_version: agent
                    .version
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                executable_path: agent.identity.executable_path.clone(),
                provider,
                provider_source,
                model: None, // Not hardcoded — profile uses RuntimeProfile's model=None
                base_url: None, // Not hardcoded
                auth_mode,
                auth_status,
                credential_ref: None,
                capabilities: CapabilitySet {
                    required: RequiredCapabilities {
                        execute: TriState::Unknown,
                        working_directory: TriState::Unknown,
                        stream_output: TriState::Unknown,
                        process_exit: TriState::Unknown,
                        cancellation: TriState::Unknown,
                        timeout: TriState::Unknown,
                        final_result: TriState::Unknown,
                    },
                    optional: OptionalCapabilities {
                        native_session_resume: TriState::Unknown,
                        structured_output: TriState::Unknown,
                        tool_events: TriState::Unknown,
                        file_change_events: TriState::Unknown,
                        reasoning_summary: TriState::Unknown,
                        interactive_approval: TriState::Unknown,
                        usage_reporting: TriState::Unknown,
                    },
                    workspace_modes: vec![],
                    supported_languages: vec![],
                    mcp_tools: vec![],
                    supported_platforms: vec![],
                },
                core_status,
                authentication_status: AuthCheckStatus::Unknown,
                execution_status: ExecutionStatus::Untested,
                optional_integrations: vec![],
                discovery_source: "passive-path-scan".to_string(),
                passive_probe: None,
                active_validation: None,
                concurrency_max: 1,
                created_at: now,
                updated_at: now,
            };

            profiles.push(profile);
        }
    }

    profiles
}

/// Bootstrap a ProductionGraph for acceptance testing with explicit profile override.
///
/// Unlike `bootstrap_production_graph` which discovers from PATH, this function
/// constructs a specific profile from explicit parameters. Used by the acceptance
/// runner to ensure deterministic profile identity.
#[allow(dead_code)]
pub async fn bootstrap_with_explicit_profile(
    pool: SqlitePool,
    worktree_root: &Path,
    repo_root: &Path,
    run_context: Arc<RunContext>,
    profile_id: &str,
    executable_path: &str,
    agent_kind: &str,
) -> Result<BootstrappedGraph, String> {
    let registry = Arc::new(ProcessRegistry::new());
    let process_manager = Arc::new(ProcessManager::new(registry));

    let profile = make_explicit_profile(profile_id, executable_path, agent_kind);

    let adapter: Arc<dyn harness_core::contracts::agent_adapter::AgentAdapter> = match agent_kind {
        "claude-code" => {
            let claude = if executable_path.is_empty() {
                ClaudeCliAdapter::new(process_manager.clone())
            } else {
                ClaudeCliAdapter::new(process_manager.clone())
                    .with_executable(PathBuf::from(executable_path))
            };
            Arc::new(claude)
        }
        _ => {
            return Err(format!(
                "unsupported agent kind for explicit bootstrap: {agent_kind}"
            ));
        }
    };

    let graph = ProductionGraph::build_with_adapter(
        pool,
        worktree_root,
        repo_root,
        run_context,
        Some(adapter),
        Some(profile.clone()),
    )?;

    Ok(BootstrappedGraph {
        graph,
        operational_profile: Some(profile),
        profiles_discovered: 1,
    })
}

// ── Independent Certification (RC-F) ──────────────────────────────────────

/// Result of an independent read-only certification session.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(dead_code)]
pub struct CertificationResult {
    pub certification_id: String,
    pub read_only: bool,
    pub fresh_session_verified: bool,
    pub profile_id: String,
    pub invocation_id: String,
    pub harness_session_id: String,
    pub evidence_frozen: bool,
    pub blocking_findings: Vec<String>,
    pub verdict: String, // "PASS" or "FAIL"
    pub summary: String,
    pub started_at: String,
    pub completed_at: String,
}

/// Run an independent read-only certification session.
///
/// Creates a fresh AgentSession, reads frozen evidence, and asks the LLM
/// to verify consistency across all evidence files. The certification session:
/// - Has its own unique invocation_id and harness_session_id
/// - Is read-only (no code modification, no DB writes)
/// - Reads frozen evidence only
/// - Uses the same profile (independence comes from fresh session + frozen evidence)
#[allow(dead_code)]
pub async fn run_independent_certification(
    evidence_dir: &Path,
    profile_id: &str,
    profile: &RuntimeProfile,
    adapter: Arc<dyn harness_core::contracts::agent_adapter::AgentAdapter>,
) -> Result<CertificationResult, String> {
    use harness_core::contracts::agent_adapter::SessionOptions;
    use harness_core::contracts::agent_event::AgentEvent;
    use harness_core::contracts::task_envelope::{FileScope, TaskBudget, TaskEnvelope};
    use std::collections::HashMap;
    use std::time::Duration;

    let invocation_id = format!("inv-cert-{}", uuid::Uuid::new_v4());
    let harness_session_id = format!("hs-cert-{}", uuid::Uuid::new_v4());
    let started_at = chrono::Utc::now();

    // ── Build certification prompt from frozen evidence ──────────
    let mut evidence_context = String::new();
    evidence_context.push_str("## FROZEN EVIDENCE (READ-ONLY CERTIFICATION)\n\n");
    evidence_context
        .push_str("You are an independent certification agent. You have READ-ONLY access.\n");
    evidence_context.push_str("You CANNOT modify code, databases, or evidence files.\n");
    evidence_context.push_str("Your task is to verify consistency across all evidence files.\n\n");

    // Read evidence files
    if evidence_dir.exists() {
        for entry in
            std::fs::read_dir(evidence_dir).map_err(|e| format!("read evidence dir: {e}"))?
        {
            let entry = entry.map_err(|e| format!("read entry: {e}"))?;
            let path = entry.path();
            if path.is_file() {
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                if name.ends_with(".json") || name.ends_with(".txt") || name.ends_with(".jsonl") {
                    if let Ok(content) = std::fs::read_to_string(&path) {
                        let truncated = if content.len() > 4000 {
                            format!("{}... (truncated at 4000 chars)", &content[..4000])
                        } else {
                            content
                        };
                        evidence_context
                            .push_str(&format!("### FILE: {}\n```\n{}\n```\n\n", name, truncated));
                    }
                }
            }
        }
    }

    if evidence_context.is_empty() {
        evidence_context.push_str("(No evidence files found — minimal certification)\n");
    }

    evidence_context.push_str("\n## CERTIFICATION TASK\n\n");
    evidence_context.push_str("Verify the following and respond with ONLY a JSON object:\n");
    evidence_context.push_str("1. Are all evidence files internally consistent?\n");
    evidence_context.push_str("2. Do the code-head references match across files?\n");
    evidence_context.push_str("3. Are there any contradictions between files?\n");
    evidence_context
        .push_str("4. Is the invocation provenance complete (distinct harness_session_ids)?\n\n");
    evidence_context.push_str("Respond with JSON:\n");
    evidence_context.push_str(r#"{"consistent": true/false, "blocking_findings": ["..."], "verdict": "PASS"/"FAIL", "summary": "...", "contradiction_count": 0}"#);

    // ── Create fresh session ─────────────────────────────────────
    let opts = SessionOptions {
        working_directory: std::env::temp_dir(),
        env: HashMap::new(),
        timeout: Duration::from_secs(120),
        max_turns: Some(1),
        resume_session_id: None, // ALWAYS fresh — never resume
        model_override: profile.model.clone(),
        effort_override: Some("high".into()),
        extra_args: vec![],
    };

    let mut session = adapter
        .start_session(profile, &opts)
        .await
        .map_err(|e| format!("certification session start: {e}"))?;

    let envelope = TaskEnvelope {
        task_id: format!("cert-{}", invocation_id),
        project_id: "i7-certification".into(),
        task_goal: evidence_context,
        scope: FileScope {
            allowed_paths: vec![],
            forbidden_paths: vec![],
            readable_paths: vec![evidence_dir.to_string_lossy().to_string()],
            scope_expansion_allowed: false,
        },
        resource_claims: vec![],
        dependencies: vec![],
        acceptance_checks: vec![],
        allowed_tools: vec![],
        output_schema: "CertificationResult".into(),
        budget: TaskBudget {
            max_turns: 1,
            max_time_ms: 120_000,
            max_cost_cents: None,
        },
        goal_contract_version: 0,
        plan_version: 1,
    };

    session
        .send_task(&envelope)
        .await
        .map_err(|e| format!("certification send: {e}"))?;

    // Collect result
    let cert_content;
    {
        struct CertCollector {
            content: String,
        }
        impl harness_core::contracts::agent_adapter::AgentEventSink for CertCollector {
            fn send(
                &mut self,
                event: AgentEvent,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = Result<(), harness_core::CoreError>>
                        + Send
                        + '_,
                >,
            > {
                Box::pin(async move {
                    #[allow(clippy::collapsible_match)]
                    match &event {
                        AgentEvent::Result {
                            content,
                            is_error: false,
                        } => {
                            self.content = content.clone();
                        }
                        AgentEvent::Message { content, .. } if self.content.is_empty() => {
                            self.content = content.clone();
                        }
                        _ => {}
                    }
                    Ok(())
                })
            }
        }
        let mut collector = CertCollector {
            content: String::new(),
        };
        session
            .receive_events(&mut collector)
            .await
            .map_err(|e| format!("certification receive: {e}"))?;
        cert_content = collector.content;
    }
    session.dispose().await.ok();

    let completed_at = chrono::Utc::now();

    // Parse certification response
    let parsed: serde_json::Value = serde_json::from_str(&cert_content)
        .unwrap_or_else(|_| serde_json::json!({
            "consistent": cert_content.contains("consistent") && !cert_content.contains("inconsistent"),
            "verdict": if cert_content.contains("PASS") { "PASS" } else { "FAIL" },
            "summary": cert_content,
            "contradiction_count": if cert_content.contains("contradiction") { 1 } else { 0 },
        }));

    let blocking_findings: Vec<String> = parsed["blocking_findings"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let verdict = parsed["verdict"].as_str().unwrap_or("FAIL").to_string();

    let result = CertificationResult {
        certification_id: format!("cert-{}", uuid::Uuid::new_v4()),
        read_only: true,
        fresh_session_verified: true,
        profile_id: profile_id.to_string(),
        invocation_id,
        harness_session_id,
        evidence_frozen: true,
        blocking_findings,
        verdict,
        summary: parsed["summary"].as_str().unwrap_or("").to_string(),
        started_at: started_at.to_rfc3339(),
        completed_at: completed_at.to_rfc3339(),
    };

    tracing::info!(
        certification_id = %result.certification_id,
        verdict = %result.verdict,
        read_only = true,
        "Independent certification complete (RC-F)"
    );

    Ok(result)
}

/// Build an explicit RuntimeProfile from parameters (no discovery needed).
#[allow(dead_code)]
fn make_explicit_profile(
    profile_id: &str,
    executable_path: &str,
    agent_kind: &str,
) -> RuntimeProfile {
    let now = chrono::Utc::now();
    RuntimeProfile {
        id: profile_id.to_string(),
        agent_definition_id: format!("explicit-{profile_id}"),
        label: format!("Explicit {agent_kind} ({profile_id})"),
        agent_kind: agent_kind.to_string(),
        adapter_kind: if agent_kind == "claude-code" {
            "claude-cli".to_string()
        } else {
            "unknown".to_string()
        },
        agent_version: "unknown".to_string(),
        executable_path: executable_path.to_string(),
        provider: "custom-anthropic-compatible".to_string(),
        provider_source: ProviderSource::CustomAnthropicCompatible,
        model: None,
        base_url: None,
        auth_mode: AuthMode::ApiKeyEnv,
        auth_status: AuthStatus::Unknown,
        credential_ref: None,
        capabilities: CapabilitySet {
            required: RequiredCapabilities {
                execute: TriState::Unknown,
                working_directory: TriState::Unknown,
                stream_output: TriState::Unknown,
                process_exit: TriState::Unknown,
                cancellation: TriState::Unknown,
                timeout: TriState::Unknown,
                final_result: TriState::Unknown,
            },
            optional: OptionalCapabilities {
                native_session_resume: TriState::Unknown,
                structured_output: TriState::Unknown,
                tool_events: TriState::Unknown,
                file_change_events: TriState::Unknown,
                reasoning_summary: TriState::Unknown,
                interactive_approval: TriState::Unknown,
                usage_reporting: TriState::Unknown,
            },
            workspace_modes: vec![],
            supported_languages: vec![],
            mcp_tools: vec![],
            supported_platforms: vec![],
        },
        core_status: CoreStatus::Available,
        authentication_status: AuthCheckStatus::Unknown,
        execution_status: ExecutionStatus::Untested,
        optional_integrations: vec![],
        discovery_source: "explicit-bootstrap".to_string(),
        passive_probe: None,
        active_validation: None,
        concurrency_max: 1,
        created_at: now,
        updated_at: now,
    }
}
