//! ProductionGoalEvaluator — calls the real Agent Adapter to produce
//! ProgressAssessmentProposals.
//!
//! Uses the existing Agent Adapter. Does NOT reimplement agent lifecycle.
//! The Evaluator only recommends; the Rust CompletionPolicy decides.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use harness_core::contracts::agent_adapter::{AgentAdapter, AgentEventSink, SessionOptions};
use harness_core::contracts::agent_event::AgentEvent;
use harness_core::contracts::runtime_profile::RuntimeProfile;
use harness_core::contracts::task_envelope::{FileScope, TaskBudget, TaskEnvelope};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;

use crate::prompt::{PromptRegistry, RenderedPrompt};

use super::service::GoalAssessmentContext;
use super::{CriterionStatus, ProgressAssessmentProposal};

/// Production Evaluator that calls a real LLM via the Agent Adapter.
pub struct ProductionGoalEvaluator {
    adapter: Arc<dyn AgentAdapter>,
    profile: RuntimeProfile,
    prompt_registry: Arc<PromptRegistry>,
    pool: SqlitePool,
    /// Invocation records for session provenance tracking.
    invocations: Arc<std::sync::Mutex<Vec<super::RoleInvocation>>>,
}

impl ProductionGoalEvaluator {
    pub fn new(
        adapter: Arc<dyn AgentAdapter>,
        profile: RuntimeProfile,
        prompt_registry: Arc<PromptRegistry>,
        pool: SqlitePool,
    ) -> Self {
        Self {
            adapter,
            profile,
            prompt_registry,
            pool,
            invocations: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Return recorded invocations (for acceptance/session provenance tracking).
    pub fn get_invocations(&self) -> Vec<super::RoleInvocation> {
        self.invocations.lock().unwrap().clone()
    }

    /// Assess goal progress by invoking the LLM.
    pub async fn assess(
        &self,
        context: &GoalAssessmentContext,
    ) -> Result<ProgressAssessmentProposal, CoreError> {
        let template = self
            .prompt_registry
            .latest("goal_evaluator")
            .ok_or_else(|| {
                CoreError::new(
                    ErrorCode::Internal,
                    "goal_evaluator prompt template not found",
                    ErrorSource::Harness,
                )
            })?;

        let input = build_evaluator_input(context);
        let input_digest = compute_digest(&input);
        let rendered = template.render(&input, &input_digest);

        let envelope = build_evaluator_envelope(&rendered);

        let output_json = self.call_adapter(&envelope).await?;

        let proposal: ProgressAssessmentProposal = serde_json::from_value(output_json.clone())
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::SerializationError,
                    format!("failed to parse ProgressAssessmentProposal: {e}"),
                    ErrorSource::Harness,
                )
            })?;

        // Rust Output Guard: reject assessments with no evidence refs
        for ca in &proposal.criteria_assessments {
            if matches!(
                ca.status,
                CriterionStatus::Satisfied | CriterionStatus::PartiallySatisfied
            ) && ca.evidence_refs.is_empty()
            {
                return Err(CoreError::new(
                    ErrorCode::InvalidState,
                    format!(
                        "criterion {} assessed as {:?} but has no evidence_refs — REJECTED by Output Guard",
                        ca.criterion_id, ca.status
                    ),
                    ErrorSource::Harness,
                ));
            }
        }

        // Reject if evaluator tries to claim goal is succeeded without evidence
        if proposal.completion_recommended
            && !proposal
                .criteria_assessments
                .iter()
                .any(|ca| matches!(ca.status, CriterionStatus::Satisfied))
        {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "evaluator recommends completion but no criteria are Satisfied — REJECTED",
                ErrorSource::Harness,
            ));
        }

        Ok(proposal)
    }

    async fn call_adapter(&self, envelope: &TaskEnvelope) -> Result<serde_json::Value, CoreError> {
        let invocation_id = format!("inv-evaluator-{}", uuid::Uuid::new_v4());
        let harness_session_id = format!("hs-evaluator-{}", uuid::Uuid::new_v4());
        let started_at = chrono::Utc::now();

        // ── Durable invocation record — written BEFORE spawn ───────
        let goal_id_fragment = &envelope.task_goal[..envelope.task_goal.len().min(64)];
        let idempotency_key = format!("evaluator-{}", &invocation_id);
        let _ = sqlx::query(
            "INSERT OR IGNORE INTO planner_invocations (invocation_id, goal_id, plan_revision_id, invocation_kind, profile_id, idempotency_key, input_digest, state, started_at, created_at) VALUES (?, ?, NULL, 'evaluator', ?, ?, ?, 'running', ?, ?)"
        )
        .bind(&invocation_id)
        .bind(goal_id_fragment)
        .bind(&self.profile.id)
        .bind(&idempotency_key)
        .bind("") // input_digest computed at call site but not available here
        .bind(started_at.to_rfc3339())
        .bind(started_at.to_rfc3339())
        .execute(&self.pool)
        .await;

        let opts = SessionOptions {
            working_directory: std::env::temp_dir(),
            env: {
                let mut m = HashMap::new();
                for key in &[
                    "ANTHROPIC_API_KEY",
                    "ANTHROPIC_BASE_URL",
                    "ANTHROPIC_MODEL",
                    "NO_PROXY",
                ] {
                    if let Ok(val) = std::env::var(key) {
                        m.insert(key.to_string(), val);
                    }
                }
                m
            },
            timeout: Duration::from_secs(120),
            max_turns: Some(1),
            resume_session_id: None,
            model_override: self.profile.model.clone(),
            effort_override: Some("high".into()),
            extra_args: vec![],
        };

        let mut session = self.adapter.start_session(&self.profile, &opts).await?;
        session.send_task(envelope).await?;

        let mut collector = EvaluatorEventCollector::new();
        let receive_result = session.receive_events(&mut collector).await;
        session.dispose().await?;

        let (result, stderr_digest, stdout_digest, exit_code, timed_out) = (
            collector.final_result,
            collector.stderr_preview,
            collector.stdout_preview,
            collector.exit_code,
            collector.timed_out,
        );

        // ── Plan evaluator outcome ─────────────────────────────────
        let eval_outcome: Result<serde_json::Value, CoreError> = match result {
            Some(Ok(value)) => Ok(value),
            Some(Err(msg)) => Err(CoreError::new(
                ErrorCode::Internal,
                format!(
                    "Evaluator result was an error: {msg} — stderr={}",
                    stderr_digest.as_deref().unwrap_or("none")
                ),
                ErrorSource::Harness,
            )),
            None => {
                let context = format!(
                    "stderr_digest={} stdout_digest={} exit_code={} timed_out={}",
                    stderr_digest.as_deref().unwrap_or("none"),
                    stdout_digest.as_deref().unwrap_or("none"),
                    exit_code.unwrap_or(-1),
                    timed_out
                );
                let err = if timed_out {
                    CoreError::new(
                        ErrorCode::ProcessTimeout {
                            duration_ms: 120_000,
                        },
                        format!("Evaluator process timeout after 120s — {context}"),
                        ErrorSource::Harness,
                    )
                } else if exit_code.is_some() && exit_code != Some(0) {
                    CoreError::new(
                        ErrorCode::Internal,
                        format!("Evaluator exited with code {} without producing final result — {context}", exit_code.unwrap()),
                        ErrorSource::Harness,
                    )
                } else {
                    CoreError::new(
                        ErrorCode::Internal,
                        format!("Evaluator produced no final result — {context}"),
                        ErrorSource::Harness,
                    )
                };
                Err(err)
            }
        };

        // ── Update durable invocation record ───────────────────────
        let completed_at = chrono::Utc::now();
        let terminal_state = if eval_outcome.is_ok() {
            "completed"
        } else {
            "failed"
        };
        let output_digest = eval_outcome.as_ref().ok().map(|output| {
            let mut h = Sha256::new();
            h.update(serde_json::to_string(output).unwrap_or_default().as_bytes());
            format!("{:x}", h.finalize())
        });

        if let Err(ref e) = receive_result {
            tracing::warn!(
                invocation_id = %invocation_id,
                error = %e,
                "Evaluator receive_events returned error"
            );
        }

        let _ = sqlx::query(
            "UPDATE planner_invocations SET state = ?, output_digest = ?, completed_at = ? WHERE invocation_id = ?"
        )
        .bind(terminal_state)
        .bind(output_digest.as_deref().unwrap_or(""))
        .bind(completed_at.to_rfc3339())
        .bind(&invocation_id)
        .execute(&self.pool)
        .await;

        let output = eval_outcome?;

        // ── Record invocation provenance ──────────────────────────
        let completed_at = chrono::Utc::now();
        let output_digest = {
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            h.update(
                serde_json::to_string(&output)
                    .unwrap_or_default()
                    .as_bytes(),
            );
            Some(format!("{:x}", h.finalize()))
        };

        let record = super::RoleInvocation {
            invocation_id: invocation_id.clone(),
            role: "GoalEvaluator".to_string(),
            profile_id: self.profile.id.clone(),
            adapter_kind: self.adapter.kind().to_string(),
            binary_path: self.profile.executable_path.clone(),
            binary_version: self.profile.agent_version.clone(),
            input_digest: String::new(), // evaluator input not tracked separately
            prompt_digest: String::new(),
            output_digest,
            harness_session_id: harness_session_id.clone(),
            vendor_session_id: Some(session.session_id().to_string()),
            session_mode: "fresh".to_string(),
            resume_requested: false,
            process_identity: format!("pid-{}", std::process::id()),
            started_at,
            completed_at: Some(completed_at),
            terminal_state: Some("completed".to_string()),
        };

        if let Ok(mut invocations) = self.invocations.lock() {
            invocations.push(record);
        }

        tracing::info!(
            invocation_id = %invocation_id,
            harness_session_id = %harness_session_id,
            role = "GoalEvaluator",
            session_mode = "fresh",
            "Evaluator invocation recorded (RC-C: session provenance)"
        );

        Ok(output)
    }
}

struct EvaluatorEventCollector {
    final_result: Option<Result<serde_json::Value, String>>,
    stdout_preview: Option<String>,
    stderr_preview: Option<String>,
    exit_code: Option<i32>,
    timed_out: bool,
}

impl EvaluatorEventCollector {
    fn new() -> Self {
        Self {
            final_result: None,
            stdout_preview: None,
            stderr_preview: None,
            exit_code: None,
            timed_out: false,
        }
    }
}

impl AgentEventSink for EvaluatorEventCollector {
    fn send(
        &mut self,
        event: AgentEvent,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), CoreError>> + Send + '_>>
    {
        Box::pin(async move {
            match &event {
                AgentEvent::Result { content, is_error } => {
                    if *is_error {
                        self.final_result = Some(Err(content.clone()));
                    } else {
                        // Robust JSON extraction — handles markdown fences,
                        // leading/trailing whitespace, and provider noise.
                        match crate::prompt::try_extract_json(content) {
                            Ok(json_str) => {
                                match serde_json::from_str::<serde_json::Value>(&json_str) {
                                    Ok(json) => {
                                        self.final_result = Some(Ok(json));
                                    }
                                    Err(e) => {
                                        self.final_result = Some(Err(format!(
                                            "JSON parse error after extraction: {e} — raw: {}",
                                            &json_str[..json_str.len().min(300)]
                                        )));
                                    }
                                }
                            }
                            Err(e) => {
                                self.final_result = Some(Err(format!(
                                    "JSON extraction failed: {e} — raw: {}",
                                    &content[..content.len().min(300)]
                                )));
                            }
                        }
                    }
                }
                AgentEvent::Error { message, .. } => {
                    if self.stderr_preview.is_none() {
                        self.stderr_preview = Some(message.clone());
                    }
                    self.final_result = Some(Err(message.clone()));
                }
                AgentEvent::ProcessExited { exit_code, .. } => {
                    self.exit_code = Some(*exit_code);
                }
                AgentEvent::SessionEnded {
                    termination_reason, ..
                } => {
                    if matches!(
                        termination_reason,
                        harness_core::contracts::agent_event::TerminationReason::Timeout
                    ) {
                        self.timed_out = true;
                    }
                }
                _ => {}
            }
            Ok(())
        })
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn build_evaluator_input(context: &GoalAssessmentContext) -> String {
    let mut input = String::new();

    input.push_str("## GOAL\n\n");
    input.push_str(&format!("Objective: {}\n", context.goal.objective));
    input.push_str(&format!("Repository HEAD: {}\n", context.repository_head));

    input.push_str("\n## SUCCESS CRITERIA TO EVALUATE\n\n");
    for c in &context.goal.success_criteria {
        let status = context
            .criteria_statuses
            .get(&c.criterion_id)
            .map(|s| format!("{:?}", s))
            .unwrap_or_else(|| "unknown".to_string());
        input.push_str(&format!(
            "- [{}] {} (required: {}, status: {})\n",
            c.criterion_id, c.description, c.required, status
        ));
    }

    input.push_str("\n## EVIDENCE LEDGER\n\n");
    for obs in &context.evidence_ledger {
        input.push_str(&format!(
            "- {} | {} | {} | {}\n",
            obs.observation_id, obs.source_aggregate_type, obs.claim, obs.evidence_type
        ));
    }
    if context.evidence_ledger.is_empty() {
        input.push_str("(no evidence collected yet)\n");
    }

    input.push_str("\n## MILESTONE STATUS\n\n");
    for m in &context.completed_milestones {
        input.push_str(&format!("- [COMPLETED] {}\n", m));
    }

    if !context.failed_tasks.is_empty() {
        input.push_str("\n## FAILED TASKS\n\n");
        for t in &context.failed_tasks {
            input.push_str(&format!("- {}\n", t));
        }
    }

    input.push_str("\n## BUDGET STATE\n\n");
    input.push_str(&format!(
        "Max plan revisions: {}\nMax total tasks: {}\n",
        context.goal.budget.max_plan_revisions, context.goal.budget.max_total_tasks,
    ));

    input.push_str("\n\n---\n**WARNING: You are an evaluator, NOT the Goal owner. You can only assess what the evidence shows. Assessments without evidence_refs will be REJECTED.**\n");

    input
}

fn build_evaluator_envelope(rendered: &RenderedPrompt) -> TaskEnvelope {
    TaskEnvelope {
        task_id: format!("evaluator-{}", uuid::Uuid::new_v4()),
        project_id: "goal-evaluator".into(),
        task_goal: rendered.full_prompt.clone(),
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
        output_schema: "ProgressAssessmentProposal".into(),
        budget: TaskBudget {
            max_turns: 1,
            max_time_ms: 120_000,
            max_cost_cents: None,
        },
        goal_contract_version: 0,
        plan_version: 1,
    }
}

fn compute_digest(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use harness_core::contracts::goal::{
        ApprovalPolicy, CriterionSubjectivity, EvidencePolicy, GoalBudget, GoalCreator, GoalSpec,
        SuccessCriterion, VerificationPolicy,
    };

    fn make_test_context() -> GoalAssessmentContext {
        GoalAssessmentContext {
            goal: GoalSpec {
                goal_id: "g1".into(),
                revision: 1,
                title: "Test".into(),
                objective: "Add a function".into(),
                repository_id: "repo-1".into(),
                target_ref: "refs/heads/main".into(),
                initial_base_head: "abc123".into(),
                success_criteria: vec![SuccessCriterion {
                    criterion_id: "c1".into(),
                    description: "Function exists".into(),
                    evidence_policy: EvidencePolicy::TaskTerminalResult,
                    verification_policy: VerificationPolicy::ExistenceOnly,
                    subjectivity: CriterionSubjectivity::Objective,
                    required: true,
                }],
                constraints: vec![],
                non_goals: vec![],
                budget: GoalBudget::default(),
                approval_policy: ApprovalPolicy::default(),
                created_by: GoalCreator::User {
                    user_id: "u1".into(),
                    user_name: None,
                },
                created_at: chrono::Utc::now(),
            },
            current_plan_revision: 1,
            evidence_ledger: vec![],
            criteria_statuses: HashMap::new(),
            completed_milestones: vec![],
            failed_tasks: vec![],
            repository_head: "abc123".into(),
        }
    }

    #[test]
    fn test_build_evaluator_input_includes_goal() {
        let ctx = make_test_context();
        let input = build_evaluator_input(&ctx);
        assert!(input.contains("Add a function"));
        assert!(input.contains("EVIDENCE LEDGER"));
    }

    #[test]
    fn test_build_evaluator_input_warns_empty_evidence() {
        let ctx = make_test_context();
        let input = build_evaluator_input(&ctx);
        assert!(input.contains("no evidence collected yet"));
    }

    #[test]
    fn test_output_guard_rejects_completion_without_satisfied() {
        let proposal = ProgressAssessmentProposal {
            schema_version: "1.0".into(),
            overall_assessment: "done".into(),
            criteria_assessments: vec![],
            plan_sufficient: true,
            replan_recommended: false,
            completion_recommended: true,
            blockers: vec![],
            summary: "all done".into(),
        };

        // Output guard logic: completion_recommended but no criteria are Satisfied
        let result = validate_proposal_static(&proposal);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("REJECTED"));
    }

    #[test]
    fn test_output_guard_accepts_completion_with_satisfied() {
        let proposal = ProgressAssessmentProposal {
            schema_version: "1.0".into(),
            overall_assessment: "done".into(),
            criteria_assessments: vec![super::super::CriterionAssessment {
                criterion_id: "c1".into(),
                status: CriterionStatus::Satisfied,
                evidence_refs: vec!["obs-1".into()],
                reason: "evidence exists".into(),
                confidence: 0.95,
                requires_human_confirmation: false,
            }],
            plan_sufficient: true,
            replan_recommended: false,
            completion_recommended: true,
            blockers: vec![],
            summary: "all done".into(),
        };

        let result = validate_proposal_static(&proposal);
        assert!(result.is_ok());
    }
}

/// Static output guard check (no adapter needed, ensures Rust validation works).
#[allow(dead_code, clippy::items_after_test_module)]
fn validate_proposal_static(proposal: &ProgressAssessmentProposal) -> Result<(), CoreError> {
    // Check evidence refs
    for ca in &proposal.criteria_assessments {
        if matches!(
            ca.status,
            CriterionStatus::Satisfied | CriterionStatus::PartiallySatisfied
        ) && ca.evidence_refs.is_empty()
        {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                format!(
                    "criterion {} assessed as {:?} but has no evidence_refs",
                    ca.criterion_id, ca.status
                ),
                ErrorSource::Harness,
            ));
        }
    }

    // Check completion without satisfied criteria
    if proposal.completion_recommended
        && !proposal
            .criteria_assessments
            .iter()
            .any(|ca| matches!(ca.status, CriterionStatus::Satisfied))
    {
        return Err(CoreError::new(
            ErrorCode::InvalidState,
            "evaluator recommends completion but no criteria are Satisfied — REJECTED",
            ErrorSource::Harness,
        ));
    }
    Ok(())
}
