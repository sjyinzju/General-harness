//! TUI projection state — pure presentation, never durable.
//!
//! The Supervisor/Repository is the source of truth; `TuiAppState` is a
//! fold over `GoalSnapshot` + `PresentationEvent`s plus local UI concerns
//! (input buffer, focus, modals). Nothing here is persisted.

use harness_core::contracts::presentation::GoalSnapshot;

use super::input::InputBuffer;
use super::spec::RepoContext;

/// Connection health as displayed in the header.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConnectionStatus {
    #[default]
    Connecting,
    Connected,
    Reconnecting,
    Disconnected,
}

impl ConnectionStatus {
    pub fn label(self) -> &'static str {
        match self {
            ConnectionStatus::Connecting => "connecting",
            ConnectionStatus::Connected => "connected",
            ConnectionStatus::Reconnecting => "reconnecting...",
            ConnectionStatus::Disconnected => "disconnected",
        }
    }
}

/// What the contextual input line submits into.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InputMode {
    /// No goal attached — Enter submits a new interactive goal.
    #[default]
    NewGoal,
    /// A new goal may be started (previous one terminal).
    Goal,
    /// Pending clarification — Enter records the current answer.
    Answer,
    /// Plan approval "request changes" mode — Enter sends feedback.
    PlanChanges,
    /// Goal active — Enter sends a runtime intervention.
    Message,
}

impl InputMode {
    pub fn prompt(self) -> &'static str {
        match self {
            InputMode::NewGoal | InputMode::Goal => "Goal",
            InputMode::Answer => "Answer",
            InputMode::PlanChanges => "Plan changes",
            InputMode::Message => "Message",
        }
    }
}

/// One line in the conversation panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationEntry {
    /// Display prefix: "You", "Harness", "Error", "Event".
    pub role: String,
    pub text: String,
    /// Occurred_at timestamp string (RFC3339), when known.
    pub at: Option<String>,
}

impl ConversationEntry {
    pub fn new(role: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            text: text.into(),
            at: None,
        }
    }

    pub fn with_at(mut self, at: impl Into<String>) -> Self {
        self.at = Some(at.into());
        self
    }
}

/// One goal row from the read-only `goal.list` projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalListItem {
    pub goal_id: String,
    pub title: String,
    pub state: String,
    pub created_at: String,
    pub updated_at: String,
}

/// A question from a clarification request, with the answer being typed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClarifyQuestionUi {
    pub question_id: String,
    pub prompt: String,
    pub choices: Vec<String>,
    pub required: bool,
    pub answer: String,
}

/// Modal / pending-interaction UI state. At most one is drawn; `render`
/// resolves precedence (fatal error > cancel confirm > clarification >
/// plan approval > help).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PendingUi {
    /// Clarification modal: approval id + per-question answers.
    pub clarify_approval_id: Option<String>,
    pub clarify_questions: Vec<ClarifyQuestionUi>,
    /// Index of the question currently being answered.
    pub clarify_index: usize,

    /// Plan approval modal: approval + bound revision (server-provided,
    /// never cached loosely — approvals bind exact revisions).
    pub approve_approval_id: Option<String>,
    pub approve_plan_revision_id: Option<String>,
    pub approve_revision_number: Option<i64>,

    /// True while the plan-approval modal is in "request changes" input mode.
    pub request_changes_mode: bool,

    /// /cancel confirmation dialog.
    pub cancel_confirm: bool,

    /// /help overlay.
    pub help_open: bool,
}

impl PendingUi {
    pub fn any_modal(&self) -> bool {
        self.clarify_approval_id.is_some()
            || self.approve_approval_id.is_some()
            || self.cancel_confirm
            || self.help_open
    }
}

/// Which panel receives scroll keys.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Focus {
    Tasks,
    Activity,
    #[default]
    Conversation,
}

/// The full TUI projection state.
#[derive(Debug, Clone, Default)]
pub struct TuiAppState {
    pub connection: ConnectionStatus,
    pub project_label: String,

    /// Goal currently attached to the shell.
    pub active_goal_id: Option<String>,
    /// Authoritative snapshot for the attached goal.
    pub snapshot: Option<GoalSnapshot>,
    /// Last applied event sequence (resume cursor).
    pub cursor: i64,
    /// Set when an event gap forces a snapshot resync.
    pub resync_needed: bool,
    /// Set when panels should refresh from a fresh snapshot (events only
    /// carry interaction truth; task detail comes from snapshots).
    pub panels_dirty: bool,

    /// Read-only goal list for `/goals` and attach-on-startup.
    pub goals: Vec<GoalListItem>,

    pub conversation: Vec<ConversationEntry>,

    pub pending: PendingUi,

    pub input_mode: InputMode,
    /// The single-line edit buffer shared by all contextual input modes.
    pub input: InputBuffer,
    pub focus: Focus,
    pub tasks_scroll: usize,
    pub activity_scroll: usize,
    pub conversation_scroll: usize,

    /// One-line toast for errors / confirmations. Never a raw Debug dump.
    pub toast: Option<String>,
    /// Toast lifetime in ticks (250ms each); cleared at zero.
    pub toast_ttl: u8,

    /// Repository context used when constructing new GoalSpecs. Supplied by
    /// the runner; opaque labels only — the TUI never runs git itself.
    pub repo_ctx: RepoContext,
    /// Monotonic counter for stable per-action idempotency keys.
    pub local_seq: u64,

    /// Exit the TUI client. Never cancels the goal.
    pub exit_requested: bool,

    /// Terminal size (columns, rows), kept from Resize events.
    pub term_cols: u16,
    pub term_rows: u16,
}

/// Minimum recommended terminal size; below this the shell draws a notice
/// instead of the full layout (never panics).
pub const MIN_COLS: u16 = 60;
pub const MIN_ROWS: u16 = 20;

impl TuiAppState {
    pub fn new(project_label: impl Into<String>) -> Self {
        Self {
            project_label: project_label.into(),
            connection: ConnectionStatus::Connecting,
            input_mode: InputMode::NewGoal,
            input: InputBuffer::new(),
            focus: Focus::Conversation,
            term_cols: 80,
            term_rows: 24,
            ..Default::default()
        }
    }

    /// Whether the attached goal is in a terminal FSM state.
    pub fn goal_is_terminal(&self) -> bool {
        self.snapshot
            .as_ref()
            .map(|s| is_terminal_goal_state(&s.goal.state))
            .unwrap_or(false)
    }

    pub fn goal_state(&self) -> Option<&str> {
        self.snapshot.as_ref().map(|s| s.goal.state.as_str())
    }
}

/// Goal FSM terminal states (mirrors `GoalState` string values).
pub fn is_terminal_goal_state(state: &str) -> bool {
    matches!(state, "succeeded" | "failed" | "cancelled" | "rejected")
}

/// Task display symbol — always paired with color so colorless terminals
/// remain readable (brief §66).
pub fn task_symbol(state: &str) -> &'static str {
    match state {
        "completed" => "✓",
        "running" => "●",
        "pending" | "materialized" => "○",
        "failed" => "×",
        "cancelled" | "superseded" => "-",
        _ => "?",
    }
}
