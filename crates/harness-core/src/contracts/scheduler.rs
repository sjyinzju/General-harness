//! Scheduler contracts — readiness states, profile selection, dispatch.
//! All types are pure data. No I/O, no ProcessManager dependencies.

use serde::{Deserialize, Serialize};

// ── Scheduler readiness ────────────────────────────────────────────────

/// Outcome of a readiness evaluation for a single Task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadyStatus {
    /// All dependencies satisfied, profile available, concurrency available.
    Ready,
    /// At least one dependency is not in a succeeded terminal state.
    Blocked {
        blocked_by: Vec<String>,
        reason: BlockReason,
    },
    /// Task is in a terminal state.
    Terminal,
    /// An active Execution already exists for this Task.
    ActiveExecutionExists { execution_id: String },
    /// No RuntimeProfile compatible with this Task's requirements.
    NoCompatibleProfile,
    /// The selected profile requires active validation before use.
    RequiresProfileValidation,
    /// Task's ResourceClaimSpec requires explicit (not derivable) claims.
    RequiresExplicitClaim,
    /// Global or profile concurrency limit reached.
    ConcurrencyLimited,
    /// Awaiting human input or approval.
    AwaitingHuman,
    /// Dependency cycle detected.
    DependencyCycle { cycle_path: Vec<String> },
    /// A referenced dependency task does not exist in the database.
    DependencyMissing { missing_ids: Vec<String> },
    /// Upstream dependency is in a failed terminal state.
    UpstreamFailed { failed_tasks: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockReason {
    DependencyIncomplete,
    DependencyFailed,
    DependencyMissing,
    DependencyCycle,
    TaskTerminal,
    ActiveExecutionExists,
    ProfileUnavailable,
    ValidationRequired,
    ExplicitClaimRequired,
    ConcurrencyLimit,
    HumanApprovalRequired,
}

// ── Profile selection ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileSelection {
    Selected {
        profile_id: String,
        agent_kind: String,
        adapter_kind: String,
        reason: String,
    },
    NoCompatibleProfile {
        required_capabilities: Vec<String>,
    },
    RequiresValidation {
        profile_id: String,
        reason: String,
    },
    ExplicitProfileUnavailable {
        requested_profile_id: String,
        reason: String,
    },
    Ambiguous {
        candidates: Vec<String>,
    },
}

// ── Concurrency reservation ────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ConcurrencyConfig {
    pub global_max: u32,
    pub per_profile_max: u32,
    pub per_repository_max: u32,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            global_max: 10,
            per_profile_max: 3,
            per_repository_max: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReservationResult {
    Reserved {
        reservation_id: String,
        profile_id: Option<String>,
        repository_id: Option<String>,
    },
    GlobalLimitReached,
    ProfileLimitReached {
        profile_id: String,
    },
    RepositoryLimitReached {
        repository_id: String,
    },
}

// ── Dispatch operation status ──────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchStatus {
    Preparing,
    WorktreeReady,
    LeaseAcquired,
    ClaimsAcquired,
    AgentStarting,
    AgentRunning,
    AgentCompleted,
    Compensating,
    Completed,
    Failed,
}

// ── Dispatch outcome ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchOutcome {
    pub dispatch_op_id: String,
    pub task_id: String,
    pub execution_id: Option<String>,
    pub status: DispatchStatus,
    pub terminal_outcome: Option<TerminalOutcome>,
    pub compensation_actions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalOutcome {
    Completed,
    ProcessFailed { exit_code: i32, reason: String },
    AdapterFailed { reason: String },
    TimedOut { duration_ms: u64 },
    Cancelled,
    Lost,
    PolicyBlocked { reason: String },
    SpawnFailed { reason: String },
}

impl TerminalOutcome {
    /// Whether this outcome requires Verification before release.
    pub fn requires_verification(&self) -> bool {
        matches!(self, Self::Completed)
    }

    /// Whether resources (Lease, Claim) should be retained after this outcome.
    pub fn retain_resources(&self) -> bool {
        self.requires_verification()
    }

    /// Whether resources should be released after this outcome.
    pub fn release_resources(&self) -> bool {
        !self.retain_resources()
    }
}

// ── Scheduler anomaly types ────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerAnomaly {
    OrphanReservation,
    TerminalExecutionWithActiveReservation,
    StaleSpawnIntent,
    TaskRunningWithoutActiveExecution,
    DuplicateActiveExecutions,
    LeaseWithoutClaim,
    ClaimWithoutLease,
    StaleFencing,
    WorktreeMissing,
    RuntimeProfileMissingOrDisabled,
    AwaitingVerificationResourcesMissing,
    TerminalEventWithoutTransition,
    FailedExecutionWithActiveLeaseOrClaim,
    ReservationWithoutTaskOrExecution,
    IncompleteSpawnIntent,
    RunningExecutionWithoutProcessRegistry,
    ProcessTerminalExecutionNonterminal,
    HeartbeatMissingForRetainedLease,
    /// DB resource_handoffs owner/status disagrees with runtime HeartbeatRegistry.
    /// May indicate: DB owner ≠ registry owner, DB Active but registry missing,
    /// registry Active but DB missing, fencing mismatch, or DB Released/Lost
    /// while registry heartbeat is still running.
    HandoffRegistryMismatch,
}
