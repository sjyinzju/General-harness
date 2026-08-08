//! Role Smoke Tests — exercise each role adapter with real Claude CLI.
//! MAX_REAL_PROVIDER_INVOCATIONS = 4 (one per role).
//! Exits 0 if all pass. Exits 1 if any fail.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use harness_core::contracts::agent_adapter::{AgentAdapter, AgentEventSink, SessionOptions};
use harness_core::contracts::agent_event::AgentEvent;
use harness_core::contracts::runtime_profile::{
    AuthMode, AuthStatus, CapabilitySet, CoreStatus, ExecutionStatus, OptionalCapabilities,
    ProviderSource, RequiredCapabilities, RuntimeProfile, TriState,
};
use harness_core::contracts::task_envelope::{FileScope, TaskBudget, TaskEnvelope};
use harness_runtime::process::manager::ProcessManager;
use harness_runtime::process::registry::ProcessRegistry;

struct SmokeCollector {
    result_received: bool,
    result_content: String,
    events_count: usize,
    exit_code: Option<i32>,
}

impl SmokeCollector {
    fn new() -> Self {
        Self {
            result_received: false,
            result_content: String::new(),
            events_count: 0,
            exit_code: None,
        }
    }
}

impl AgentEventSink for SmokeCollector {
    fn send(
        &mut self,
        event: AgentEvent,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), harness_core::CoreError>> + Send + '_>,
    > {
        Box::pin(async move {
            self.events_count += 1;
            match &event {
                AgentEvent::Result { content, .. } => {
                    self.result_received = true;
                    self.result_content = content.clone();
                }
                AgentEvent::ProcessExited { exit_code, .. } => {
                    self.exit_code = Some(*exit_code);
                }
                _ => {}
            }
            Ok(())
        })
    }
}

fn make_profile() -> RuntimeProfile {
    let now = chrono::Utc::now();
    RuntimeProfile {
        id: "role-smoke".into(),
        agent_definition_id: "smoke".into(),
        label: "Role Smoke".into(),
        agent_kind: "claude-code".into(),
        adapter_kind: "claude-cli".into(),
        agent_version: "unknown".into(),
        executable_path: r"C:\Users\shiju\AppData\Roaming\npm\claude.cmd".into(),
        provider: "custom-anthropic-compatible".into(),
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
        authentication_status: harness_core::contracts::runtime_profile::AuthCheckStatus::Unknown,
        execution_status: ExecutionStatus::Untested,
        optional_integrations: vec![],
        discovery_source: "role-smoke".into(),
        passive_probe: None,
        active_validation: None,
        concurrency_max: 1,
        created_at: now,
        updated_at: now,
    }
}

fn make_opts() -> SessionOptions {
    let mut env = HashMap::new();
    for key in &[
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_MODEL",
        "NO_PROXY",
    ] {
        if let Ok(val) = std::env::var(key) {
            env.insert(key.to_string(), val);
        }
    }
    SessionOptions {
        working_directory: std::env::temp_dir(),
        env,
        timeout: Duration::from_secs(60),
        max_turns: Some(1),
        resume_session_id: None,
        model_override: None,
        effort_override: None,
        extra_args: vec![],
    }
}

async fn run_role_smoke(label: &str, task_goal: &str) -> Result<(), String> {
    let registry = Arc::new(ProcessRegistry::new());
    let pm = Arc::new(ProcessManager::new(registry));
    let adapter: Arc<dyn AgentAdapter> = Arc::new(harness_adapters::ClaudeCliAdapter::new(pm));
    let profile = make_profile();
    let opts = make_opts();

    let envelope = TaskEnvelope {
        task_id: format!("smoke-{}", label.to_lowercase()),
        project_id: "smoke".into(),
        task_goal: task_goal.to_string(),
        scope: FileScope {
            allowed_paths: vec![],
            forbidden_paths: vec![],
            readable_paths: vec![],
            scope_expansion_allowed: false,
        },
        resource_claims: vec![],
        dependencies: vec![],
        acceptance_checks: vec![],
        allowed_tools: vec![],
        output_schema: String::new(),
        budget: TaskBudget {
            max_turns: 1,
            max_time_ms: 60_000,
            max_cost_cents: None,
        },
        goal_contract_version: 0,
        plan_version: 1,
    };

    let mut session = adapter
        .start_session(&profile, &opts)
        .await
        .map_err(|e| format!("start session: {e}"))?;
    session
        .send_task(&envelope)
        .await
        .map_err(|e| format!("send task: {e}"))?;

    let mut collector = SmokeCollector::new();
    session
        .receive_events(&mut collector)
        .await
        .map_err(|e| format!("receive events: {e}"))?;
    session.dispose().await.ok();

    if !collector.result_received {
        return Err(format!(
            "no Result event (events={}, exit_code={:?})",
            collector.events_count, collector.exit_code
        ));
    }
    println!(
        "  [{label}] PASS — {} events, result: {}",
        collector.events_count,
        &collector.result_content[..collector.result_content.len().min(120)]
    );
    Ok(())
}

#[tokio::main]
async fn main() {
    println!("=== ROLE SMOKE TESTS ===");
    println!("MAX_REAL_PROVIDER_INVOCATIONS: 4\n");

    let mut failures = 0u32;

    // ── Planner ────────────────────────────────────────────────────
    print!("[1/4] Planner ... ");
    match run_role_smoke(
        "Planner",
        "You are a planner. Respond with exactly this JSON and nothing else: {\"answer\":\"plan created\",\"tasks\":[{\"id\":\"t1\",\"title\":\"test\"}]}",
    )
    .await
    {
        Ok(()) => {}
        Err(e) => {
            println!("FAIL: {e}");
            failures += 1;
        }
    }

    // ── Executor ───────────────────────────────────────────────────
    print!("[2/4] Executor ... ");
    match run_role_smoke(
        "Executor",
        "You are an executor. Respond with exactly this JSON and nothing else: {\"status\":\"completed\",\"summary\":\"task done\"}",
    )
    .await
    {
        Ok(()) => {}
        Err(e) => {
            println!("FAIL: {e}");
            failures += 1;
        }
    }

    // ── Reviewer ───────────────────────────────────────────────────
    print!("[3/4] Reviewer ... ");
    match run_role_smoke(
        "Reviewer",
        "You are a code reviewer. Respond with exactly this JSON and nothing else: {\"decision\":\"approved\",\"comments\":\"looks good\"}",
    )
    .await
    {
        Ok(()) => {}
        Err(e) => {
            println!("FAIL: {e}");
            failures += 1;
        }
    }

    // ── Evaluator ─────────────────────────────────────────────────
    print!("[4/4] Evaluator ... ");
    match run_role_smoke(
        "Evaluator",
        "You are an evaluator. Respond with exactly this JSON and nothing else: {\"assessment\":\"on_track\",\"completion_pct\":50}",
    )
    .await
    {
        Ok(()) => {}
        Err(e) => {
            println!("FAIL: {e}");
            failures += 1;
        }
    }

    println!();
    if failures == 0 {
        println!("=== ALL 4 ROLE SMOKES PASS ===");
    } else {
        eprintln!("=== {failures}/4 ROLE SMOKES FAILED ===");
        std::process::exit(1);
    }
}
