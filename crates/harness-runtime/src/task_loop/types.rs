//! Domain types for I4.5 Task Engineering Loop.

use serde::{Deserialize, Serialize};

// ── Loop lifecycle ──────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoopLifecycle {
    Created,
    Ready,
    PreparingAttempt,
    AttemptActive,
    Evaluating,
    CompleteCandidate,
    WaitingForReconciliation,
    WaitingForInfrastructure,
    WaitingForHuman,
    BudgetExhausted,
    NoProgress,
    NonRetryable,
    Escalated,
    Cancelled,
    ReconciliationRequired,
    Failed,
}

impl LoopLifecycle {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Ready => "ready",
            Self::PreparingAttempt => "preparing_attempt",
            Self::AttemptActive => "attempt_active",
            Self::Evaluating => "evaluating",
            Self::CompleteCandidate => "complete_candidate",
            Self::WaitingForReconciliation => "waiting_for_reconciliation",
            Self::WaitingForInfrastructure => "waiting_for_infrastructure",
            Self::WaitingForHuman => "waiting_for_human",
            Self::BudgetExhausted => "budget_exhausted",
            Self::NoProgress => "no_progress",
            Self::NonRetryable => "non_retryable",
            Self::Escalated => "escalated",
            Self::Cancelled => "cancelled",
            Self::ReconciliationRequired => "reconciliation_required",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "ready" => Self::Ready,
            "preparing_attempt" => Self::PreparingAttempt,
            "attempt_active" => Self::AttemptActive,
            "evaluating" => Self::Evaluating,
            "complete_candidate" => Self::CompleteCandidate,
            "waiting_for_reconciliation" => Self::WaitingForReconciliation,
            "waiting_for_infrastructure" => Self::WaitingForInfrastructure,
            "waiting_for_human" => Self::WaitingForHuman,
            "budget_exhausted" => Self::BudgetExhausted,
            "no_progress" => Self::NoProgress,
            "non_retryable" => Self::NonRetryable,
            "escalated" => Self::Escalated,
            "cancelled" => Self::Cancelled,
            "reconciliation_required" => Self::ReconciliationRequired,
            "failed" => Self::Failed,
            _ => Self::Created,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::CompleteCandidate
                | Self::BudgetExhausted
                | Self::NoProgress
                | Self::NonRetryable
                | Self::Escalated
                | Self::Cancelled
                | Self::Failed
        )
    }

    /// Human-decision states: not auto-resumed.
    pub fn requires_human(&self) -> bool {
        matches!(self, Self::WaitingForHuman)
    }

    /// States that prevent creating a new Attempt.
    pub fn blocks_new_attempt(&self) -> bool {
        !matches!(
            self,
            Self::Ready | Self::PreparingAttempt | Self::Evaluating
        )
    }
}

// ── Attempt lifecycle ───────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptLifecycle {
    Created,
    Prepared,
    Dispatched,
    Executing,
    Terminal,
    Cancelled,
    Failed,
}

impl AttemptLifecycle {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Prepared => "prepared",
            Self::Dispatched => "dispatched",
            Self::Executing => "executing",
            Self::Terminal => "terminal",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "prepared" => Self::Prepared,
            "dispatched" => Self::Dispatched,
            "executing" => Self::Executing,
            "terminal" => Self::Terminal,
            "cancelled" => Self::Cancelled,
            "failed" => Self::Failed,
            _ => Self::Created,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminal | Self::Cancelled | Self::Failed)
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Dispatched | Self::Executing)
    }
}

// ── Decision classification ─────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecisionClassification {
    CompleteCandidate,
    ContinueRepair,
    AwaitingReconciliation,
    InfrastructureBlocked,
    AwaitingHuman,
    BudgetExhausted,
    NoProgress,
    NonRetryable,
    Cancelled,
    EscalateToProjectPlanner,
}

impl DecisionClassification {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CompleteCandidate => "CompleteCandidate",
            Self::ContinueRepair => "ContinueRepair",
            Self::AwaitingReconciliation => "AwaitingReconciliation",
            Self::InfrastructureBlocked => "InfrastructureBlocked",
            Self::AwaitingHuman => "AwaitingHuman",
            Self::BudgetExhausted => "BudgetExhausted",
            Self::NoProgress => "NoProgress",
            Self::NonRetryable => "NonRetryable",
            Self::Cancelled => "Cancelled",
            Self::EscalateToProjectPlanner => "EscalateToProjectPlanner",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "CompleteCandidate" => Some(Self::CompleteCandidate),
            "ContinueRepair" => Some(Self::ContinueRepair),
            "AwaitingReconciliation" => Some(Self::AwaitingReconciliation),
            "InfrastructureBlocked" => Some(Self::InfrastructureBlocked),
            "AwaitingHuman" => Some(Self::AwaitingHuman),
            "BudgetExhausted" => Some(Self::BudgetExhausted),
            "NoProgress" => Some(Self::NoProgress),
            "NonRetryable" => Some(Self::NonRetryable),
            "Cancelled" => Some(Self::Cancelled),
            "EscalateToProjectPlanner" => Some(Self::EscalateToProjectPlanner),
            _ => None,
        }
    }

    /// Formal action that follows this decision.
    pub fn action(&self) -> &'static str {
        match self {
            Self::CompleteCandidate => "none",
            Self::ContinueRepair => "create_attempt",
            Self::AwaitingReconciliation => "wait_reconciliation",
            Self::InfrastructureBlocked => "wait_infrastructure",
            Self::AwaitingHuman => "wait_human",
            Self::BudgetExhausted => "stop",
            Self::NoProgress => "stop",
            Self::NonRetryable => "stop",
            Self::Cancelled => "stop",
            Self::EscalateToProjectPlanner => "escalate",
        }
    }

    /// Whether this decision creates a new Attempt.
    pub fn creates_attempt(&self) -> bool {
        matches!(self, Self::ContinueRepair)
    }
}

// ── Workspace source kind ───────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceSourceKind {
    Initial,
    ContinueFromAttempt,
}

impl WorkspaceSourceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::ContinueFromAttempt => "continue_from_attempt",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "continue_from_attempt" => Self::ContinueFromAttempt,
            _ => Self::Initial,
        }
    }
}

// ── Operation kind ──────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopOperationKind {
    CreateLoop,
    AcquireLoopOwnership,
    PrepareAttempt,
    CreateExecution,
    DispatchAttempt,
    ObserveAttemptOutcome,
    RecordDecision,
    CreateContextPack,
    AdvanceLoop,
    CancelLoop,
    ReconcileLoop,
}

impl LoopOperationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CreateLoop => "create_loop",
            Self::AcquireLoopOwnership => "acquire_loop_ownership",
            Self::PrepareAttempt => "prepare_attempt",
            Self::CreateExecution => "create_execution",
            Self::DispatchAttempt => "dispatch_attempt",
            Self::ObserveAttemptOutcome => "observe_attempt_outcome",
            Self::RecordDecision => "record_decision",
            Self::CreateContextPack => "create_context_pack",
            Self::AdvanceLoop => "advance_loop",
            Self::CancelLoop => "cancel_loop",
            Self::ReconcileLoop => "reconcile_loop",
        }
    }
}

// ── Budget policy ───────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BudgetMode {
    Hard,
    Advisory,
    ObserveOnly,
    #[default]
    Unset,
}

impl BudgetMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Hard => "hard",
            Self::Advisory => "advisory",
            Self::ObserveOnly => "observe_only",
            Self::Unset => "unset",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "hard" => Some(Self::Hard),
            "advisory" => Some(Self::Advisory),
            "observe_only" => Some(Self::ObserveOnly),
            "unset" => Some(Self::Unset),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum UnknownUsagePolicy {
    BlockUnknown,
    #[default]
    AllowWithWarning,
    AwaitHuman,
}

impl UnknownUsagePolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BlockUnknown => "block_unknown",
            Self::AllowWithWarning => "allow_with_warning",
            Self::AwaitHuman => "await_human",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "block_unknown" => Some(Self::BlockUnknown),
            "allow_with_warning" => Some(Self::AllowWithWarning),
            "await_human" => Some(Self::AwaitHuman),
            _ => None,
        }
    }
}

// ── Progress classification ─────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProgressVerdict {
    Progress,
    PartialProgress,
    NoProgress,
    Regression,
    CycleDetected,
}

impl ProgressVerdict {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Progress => "progress",
            Self::PartialProgress => "partial_progress",
            Self::NoProgress => "no_progress",
            Self::Regression => "regression",
            Self::CycleDetected => "cycle_detected",
        }
    }
}

// ── Row types ───────────────────────────────────────────────────────

/// Persisted task engineering loop row.
#[derive(Debug, Clone)]
pub struct TaskLoopRow {
    pub loop_id: String,
    pub project_id: String,
    pub task_id: String,
    pub lifecycle: LoopLifecycle,
    pub policy_json: String,
    pub policy_fingerprint: Option<String>,
    pub idempotency_key: String,
    pub request_hash: String,
    pub owner_id: Option<String>,
    pub fencing_token: i64,
    pub lease_expires_at: Option<String>,
    pub active_attempt_id: Option<String>,
    pub current_attempt_ordinal: i64,
    pub attempt_count: i64,
    pub no_progress_streak: i64,
    pub same_failure_streak: i64,
    pub profile_switch_count: i64,
    pub started_at: Option<String>,
    pub updated_at: String,
    pub terminal_at: Option<String>,
    pub last_error_classification: Option<String>,
    pub version: i64,
}

/// Persisted task engineering attempt row.
#[derive(Debug, Clone)]
pub struct TaskAttemptRow {
    pub attempt_id: String,
    pub loop_id: String,
    pub ordinal: i64,
    pub parent_attempt_id: Option<String>,
    pub execution_id: Option<String>,
    pub verification_run_id: Option<String>,
    pub context_pack_id: Option<String>,
    pub runtime_profile_id: String,
    pub workspace_source_kind: WorkspaceSourceKind,
    pub source_execution_id: Option<String>,
    pub source_worktree_id: Option<String>,
    pub source_baseline_commit: Option<String>,
    pub source_head: Option<String>,
    pub source_diff_fingerprint: Option<String>,
    pub lifecycle: AttemptLifecycle,
    pub outcome_kind: Option<String>,
    pub outcome_fingerprint: Option<String>,
    pub dossier_fingerprint: Option<String>,
    pub decision_id: Option<String>,
    pub started_at: Option<String>,
    pub terminal_at: Option<String>,
    pub version: i64,
}

/// Persisted decision row.
#[derive(Debug, Clone)]
pub struct DecisionRow {
    pub decision_id: String,
    pub loop_id: String,
    pub attempt_id: String,
    pub classification: DecisionClassification,
    pub action: String,
    pub reason_codes_json: String,
    pub observed_state_fingerprint: Option<String>,
    pub outcome_fingerprint: Option<String>,
    pub dossier_fingerprint: Option<String>,
    pub progress_fingerprint: Option<String>,
    pub budget_snapshot_fingerprint: Option<String>,
    pub selected_profile_id: Option<String>,
    pub next_context_pack_id: Option<String>,
    pub idempotency_key: String,
    pub request_hash: String,
    pub created_at: String,
}

/// Persisted context pack row.
#[derive(Debug, Clone)]
pub struct ContextPackRow {
    pub context_pack_id: String,
    pub loop_id: String,
    pub source_attempt_id: Option<String>,
    pub target_attempt_ordinal: i64,
    pub schema_version: i64,
    pub payload_json: String,
    pub source_fingerprints_json: String,
    pub context_fingerprint: String,
    pub estimated_input_tokens: Option<i64>,
    pub validation_status: String,
    pub created_at: String,
}

/// Persisted usage ledger row.
#[derive(Debug, Clone)]
pub struct UsageLedgerRow {
    pub usage_id: String,
    pub loop_id: String,
    pub attempt_id: String,
    pub execution_id: Option<String>,
    pub runtime_profile_id: String,
    pub model_identifier: Option<String>,
    pub provider_identifier: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub tool_calls: Option<i64>,
    pub wall_time_ms: Option<i64>,
    pub estimated_cost_micros: Option<i64>,
    pub usage_source: String,
    pub usage_known: bool,
    pub usage_fingerprint: Option<String>,
    pub idempotency_key: String,
    pub created_at: String,
}

/// Persisted loop operation row.
#[derive(Debug, Clone)]
pub struct LoopOperationRow {
    pub operation_id: String,
    pub loop_id: String,
    pub operation_kind: LoopOperationKind,
    pub idempotency_key: String,
    pub request_hash: String,
    pub observed_state_fingerprint: Option<String>,
    pub lifecycle: String,
    pub owner_id: Option<String>,
    pub fencing_token: Option<i64>,
    pub result_fingerprint: Option<String>,
    pub started_at: String,
    pub terminal_at: Option<String>,
    pub last_error_classification: Option<String>,
    pub version: i64,
}

// ── Request / outcome types ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateLoopRequest {
    pub project_id: String,
    pub task_id: String,
    pub policy_json: String,
    pub policy_fingerprint: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub owner_id: String,
    pub lease_secs: u32,
}

#[derive(Debug, Clone)]
pub enum CreateLoopOutcome {
    Created { loop_id: String },
    Duplicate { loop_id: String },
    IdempotencyConflict { existing_hash: String },
    TaskAlreadyHasActiveLoop { existing_loop_id: String },
    InfrastructureError { reason: String },
}

#[derive(Debug, Clone)]
pub struct PrepareAttemptRequest {
    pub loop_id: String,
    pub loop_version: i64,
    pub owner_id: String,
    pub fencing_token: i64,
    pub runtime_profile_id: String,
    pub workspace_source: AttemptWorkspaceSource,
    pub idempotency_key: String,
    pub request_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AttemptWorkspaceSource {
    InitialTaskWorkspace {
        repository_path: String,
    },
    ContinueFromAttempt {
        source_attempt_id: String,
        source_execution_id: String,
        source_worktree_id: String,
        expected_baseline_commit: String,
        expected_head: String,
        expected_diff_fingerprint: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextPackSpec {
    pub task_id: String,
    pub task_goal: String,
    pub acceptance_criteria: String,
    pub attempt_ordinal: i64,
    pub workspace_continuation: AttemptWorkspaceSource,
    pub previous_outcome: Option<String>,
    pub primary_failure_classification: Option<String>,
    pub all_blockers: Vec<String>,
    pub failed_required_steps: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub changed_files: Vec<String>,
    pub do_not_repeat_fingerprints: Vec<String>,
    pub required_next_objective: Option<String>,
    pub remaining_budget_facts: Option<String>,
    pub runtime_profile_id: String,
    pub stop_conditions: Vec<String>,
}

// ── FNV-1a helper ──────────────────────────────────────────────────

/// Deterministic, platform-stable FNV-1a 64-bit hash.
pub fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Produce a hex fingerprint string from a canonical source.
pub fn fingerprint_hex(source: &str) -> String {
    format!("{:016x}", fnv1a64(source))
}
