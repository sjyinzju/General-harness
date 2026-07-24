//! Supervisor domain types — SupervisorInstance, state machine, events.
//!
//! These are pure domain types with ZERO I/O dependencies.
//! The runtime crate provides persistence and the service implementation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Unique identifier for a Supervisor instance (one boot of the harness binary).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SupervisorInstanceId(pub String);

impl std::fmt::Display for SupervisorInstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for SupervisorInstanceId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SupervisorInstanceId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// The lifecycle state of a Supervisor instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorState {
    /// Initial state — instance record created but not yet started.
    Created,
    /// Starting: performing pre-flight checks, opening DB, running migrations.
    Starting,
    /// Attempting to acquire exclusive ownership (lease + fencing).
    AcquiringOwnership,
    /// Recovering: scanning incomplete operations and reconciling state.
    Recovering,
    /// Ready: accepting IPC commands and executing the control loop.
    Ready,
    /// Draining: no new work accepted, waiting for active operations to finish.
    Draining,
    /// Stopping: final cleanup, releasing lease, closing IPC.
    Stopping,
    /// Terminal: clean shutdown complete.
    Stopped,
    /// Terminal: unrecoverable failure.
    Failed,
    /// Transitional: detected a stale owner, preparing to take over.
    TakingOver,
}

impl SupervisorState {
    /// Returns true if this is a terminal state that cannot be changed.
    pub fn is_terminal(self) -> bool {
        matches!(self, SupervisorState::Stopped | SupervisorState::Failed)
    }

    /// Returns true if the supervisor is in an active (running) state.
    pub fn is_active(self) -> bool {
        matches!(
            self,
            SupervisorState::Ready
                | SupervisorState::Recovering
                | SupervisorState::AcquiringOwnership
                | SupervisorState::Draining
        )
    }

    /// Returns true if this state can accept new write operations.
    pub fn accepts_writes(self) -> bool {
        matches!(self, SupervisorState::Ready)
    }

    /// Returns true if this state can accept health/status checks.
    pub fn accepts_health(self) -> bool {
        matches!(
            self,
            SupervisorState::Starting
                | SupervisorState::AcquiringOwnership
                | SupervisorState::Recovering
                | SupervisorState::Ready
                | SupervisorState::Draining
                | SupervisorState::Stopping
        )
    }
}

impl std::fmt::Display for SupervisorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SupervisorState::Created => write!(f, "created"),
            SupervisorState::Starting => write!(f, "starting"),
            SupervisorState::AcquiringOwnership => write!(f, "acquiring_ownership"),
            SupervisorState::Recovering => write!(f, "recovering"),
            SupervisorState::Ready => write!(f, "ready"),
            SupervisorState::Draining => write!(f, "draining"),
            SupervisorState::Stopping => write!(f, "stopping"),
            SupervisorState::Stopped => write!(f, "stopped"),
            SupervisorState::Failed => write!(f, "failed"),
            SupervisorState::TakingOver => write!(f, "taking_over"),
        }
    }
}

/// A Supervisor instance record — one row per boot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorInstance {
    /// Unique instance identifier (UUID v4).
    pub instance_id: SupervisorInstanceId,
    /// Logical directory scope for this supervisor.
    /// Only one active instance per state_directory_id.
    pub state_directory_id: String,

    /// OS process ID of the supervisor binary.
    pub pid: u32,
    /// When the supervisor process was started (OS creation time).
    pub process_started_at: DateTime<Utc>,
    /// A random nonce generated at boot for additional identity verification.
    pub boot_nonce: String,

    /// Current lifecycle state.
    pub state: SupervisorState,
    /// Monotonic fencing token — incremented on each takeover.
    pub fencing_token: i64,

    /// When this instance first started.
    pub started_at: DateTime<Utc>,
    /// Last heartbeat timestamp.
    pub heartbeat_at: DateTime<Utc>,
    /// When the current lease expires.
    pub lease_expires_at: DateTime<Utc>,

    /// IPC protocol version supported by this instance.
    pub protocol_version: String,
    /// Harness binary version string.
    pub binary_version: String,
}

/// A lease record for the Supervisor — used for exclusive ownership.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorLease {
    /// The state_directory_id this lease covers.
    pub state_directory_id: String,
    /// The instance that holds this lease.
    pub instance_id: SupervisorInstanceId,
    /// Current fencing token value.
    pub fencing_token: i64,
    /// When this lease was acquired.
    pub acquired_at: DateTime<Utc>,
    /// When this lease expires (heartbeat must refresh before this).
    pub expires_at: DateTime<Utc>,
    /// Whether this lease is currently active.
    pub is_active: bool,
}

/// Supervisor lifecycle events (append-only, durable).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum SupervisorEvent {
    SupervisorStarting {
        instance_id: SupervisorInstanceId,
        pid: u32,
        process_started_at: DateTime<Utc>,
        occurred_at: DateTime<Utc>,
    },
    SupervisorOwnershipAcquired {
        instance_id: SupervisorInstanceId,
        fencing_token: i64,
        occurred_at: DateTime<Utc>,
    },
    SupervisorOwnershipRejected {
        instance_id: SupervisorInstanceId,
        reason: String,
        active_instance_id: Option<SupervisorInstanceId>,
        occurred_at: DateTime<Utc>,
    },
    SupervisorHeartbeat {
        instance_id: SupervisorInstanceId,
        fencing_token: i64,
        occurred_at: DateTime<Utc>,
    },
    SupervisorReady {
        instance_id: SupervisorInstanceId,
        occurred_at: DateTime<Utc>,
    },
    SupervisorDraining {
        instance_id: SupervisorInstanceId,
        reason: String,
        occurred_at: DateTime<Utc>,
    },
    SupervisorStopping {
        instance_id: SupervisorInstanceId,
        occurred_at: DateTime<Utc>,
    },
    SupervisorStopped {
        instance_id: SupervisorInstanceId,
        occurred_at: DateTime<Utc>,
    },
    SupervisorLeaseLost {
        instance_id: SupervisorInstanceId,
        reason: String,
        occurred_at: DateTime<Utc>,
    },
    SupervisorStaleOwnerDetected {
        instance_id: SupervisorInstanceId,
        stale_instance_id: SupervisorInstanceId,
        stale_fencing_token: i64,
        occurred_at: DateTime<Utc>,
    },
    SupervisorTakeoverStarted {
        instance_id: SupervisorInstanceId,
        previous_instance_id: SupervisorInstanceId,
        new_fencing_token: i64,
        occurred_at: DateTime<Utc>,
    },
    SupervisorTakeoverCompleted {
        instance_id: SupervisorInstanceId,
        new_fencing_token: i64,
        occurred_at: DateTime<Utc>,
    },
    SupervisorFailed {
        instance_id: SupervisorInstanceId,
        reason: String,
        occurred_at: DateTime<Utc>,
    },
}

/// Supervisor health/status snapshot for CLI reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorStatus {
    pub instance_id: SupervisorInstanceId,
    pub state: SupervisorState,
    pub pid: u32,
    pub process_started_at: DateTime<Utc>,
    pub fencing_token: i64,
    pub started_at: DateTime<Utc>,
    pub heartbeat_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
    pub uptime_secs: u64,
    pub protocol_version: String,
    pub active_operations: u32,
    pub running_processes: u32,
    pub queue_depth: u32,
    pub active_claims: u32,
    pub active_leases: u32,
    pub recovery_state: Option<String>,
    pub last_recovery_summary: Option<String>,
    pub janitor_status: String,
    pub ipc_connections: u32,
    pub orphan_count: u32,
}

/// Configuration for the Supervisor runtime.
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// How long a lease lasts before expiry (no heartbeat).
    pub lease_duration_secs: u64,
    /// How often to send heartbeats.
    pub heartbeat_interval_secs: u64,
    /// Maximum number of concurrent operations.
    pub max_operation_concurrency: usize,
    /// Grace period for shutdown before force-terminating operations.
    pub shutdown_grace_period_secs: u64,
    /// Maximum IPC frame size in bytes.
    pub max_ipc_frame_bytes: usize,
    /// Maximum number of concurrent IPC connections.
    pub max_ipc_connections: usize,
    /// Maximum outstanding IPC requests.
    pub max_inflight_requests: usize,
    /// Maximum event stream buffer size.
    pub max_event_stream_buffer: usize,
    /// Maximum diagnostic output bytes.
    pub max_diagnostic_bytes: usize,
    /// State directory ID for this supervisor instance.
    pub state_directory_id: String,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            lease_duration_secs: 30,
            heartbeat_interval_secs: 10,
            max_operation_concurrency: 8,
            shutdown_grace_period_secs: 30,
            max_ipc_frame_bytes: 16 * 1024 * 1024, // 16 MiB
            max_ipc_connections: 32,
            max_inflight_requests: 64,
            max_event_stream_buffer: 1024,
            max_diagnostic_bytes: 1024 * 1024, // 1 MiB
            state_directory_id: "default".to_string(),
        }
    }
}
