pub mod agent_adapter;
pub mod agent_definition;
pub mod agent_event;
pub mod agent_identity;
pub mod candidate;
pub mod discovery;
pub mod goal_contract;
pub mod project;
pub mod repository;
pub mod review;
pub mod runtime_profile;
pub mod scheduler;
pub mod task;
pub mod task_envelope;
pub mod task_result;
pub mod verification;
pub mod workspace;

pub use agent_adapter::{
    AgentAdapter, AgentEventSink, AgentSession, DetectionResult, SessionOptions,
};
pub use agent_definition::{AgentDefinition, DiscoverySource, PassiveDiscoveryStatus};
pub use agent_event::AgentEvent;
pub use agent_identity::AgentIdentity;
pub use candidate::{CandidateId, CandidateSnapshot};
pub use discovery::{
    ActiveValidationRequest, AdapterCompatibility, AuthModeHint, AuthStateValue,
    AuthenticationState, CapabilityNegotiation, CapabilitySupport, CompatibilityDiagnostic,
    DiagnosticLevel, DiscoveredAgent, DiscoveryConfidence, DiscoveryEvidence, EvidenceKind,
    ExecutableIdentity, ProviderHint, ProviderHintSource, ValidationResult, ValidationStatus,
};
pub use goal_contract::{ChangeRequest, GoalContractVersion};
pub use project::{Project, ProjectLifecycle};
pub use review::{
    ApprovedCandidate, FindingCategory, FindingSeverity, PrecheckFinding, PrecheckResult,
    ReviewCacheKey, ReviewConfig, ReviewDecision, ReviewDossier, ReviewFinding, ReviewRequest,
    ReviewState, ReviewerFinding, ReviewerOutput,
};
pub use runtime_profile::{
    ActiveValidationResult as ProbeResult, CapabilitySet, CoreStatus,
    CoreStatus as RuntimeProfileStatus, OptionalCapabilities, RequiredCapabilities, RuntimeProfile,
    TriState,
};
pub use task::{Task, TaskDependency, TaskLifecycle};
pub use task_envelope::{FileScope, TaskBudget, TaskEnvelope};
pub use task_result::TaskResult;
pub use workspace::{LeaseLifecycle, WorkspaceLease};
