//! TUI actions (inbound) and effects (outbound intents).
//!
//! The reducer is a pure function `reduce(state, action) -> Vec<Effect>`;
//! the runner executes effects through the IPC gateway. User text is data:
//! effects only ever name IPC commands, never shell commands.

use harness_core::contracts::presentation::{GoalSnapshot, PresentationEvent};

use super::state::GoalListItem;

/// Inbound actions consumed by the reducer.
#[derive(Debug, Clone)]
pub enum TuiAction {
    /// Terminal was resized.
    Resize { cols: u16, rows: u16 },
    /// Periodic UI tick (elapsed-time refresh, dirty-panel refetch).
    Tick,

    /// IPC connection became healthy.
    Connected,
    /// IPC connection lost (transport error).
    Disconnected { reason: String },

    /// Read-only `goal.list` result.
    GoalsListed { goals: Vec<GoalListItem> },
    /// `goal.snapshot` result for the attached goal.
    SnapshotReceived { snapshot: Box<GoalSnapshot> },
    /// A batch from the `goal.events` long-poll.
    EventsReceived { events: Vec<PresentationEvent> },

    /// An IPC mutation completed with Success or Duplicate (replay).
    MutationAcked {
        slot: MutationSlot,
        payload: serde_json::Value,
    },
    /// An IPC mutation failed with a structured error.
    MutationFailed { slot: MutationSlot, message: String },
    /// Idempotency Conflict — must be surfaced, never silently retried.
    MutationConflict { slot: MutationSlot, message: String },
    /// A read-only request failed — surfaced, but carries no mutation slot.
    Notice { message: String },

    /// The user pressed Enter on the input line.
    Submit,
    /// A single printable character typed into the active edit field.
    CharInput(char),
    /// Named key intents produced by the terminal event task.
    Key(KeyIntent),

    /// Exit requested (Ctrl+C / /quit). Local only — never cancels a goal.
    Quit,
}

/// Key intents (transport-agnostic so the reducer stays unit-testable).
/// Modal decisions travel as plain characters (`a`/`e`/`x`, `y`/`n`) so
/// they work on every keyboard layout; intents cover editing/navigation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyIntent {
    Backspace,
    Delete,
    Left,
    Right,
    Home,
    End,
    Up,
    Down,
    PageUp,
    PageDown,
    Tab,
    Esc,
    /// Ctrl+U — clear the current line.
    ClearLine,
}

/// Logical mutation slots — one stable idempotency key per user action.
/// Retries after timeout/disconnect reuse the slot's key and payload so the
/// Request Ledger replays instead of double-applying.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MutationSlot {
    GoalCreate,
    GoalStart,
    Answer,
    Approve,
    RequestChanges,
    Reject,
    Intervene,
    Pause,
    Resume,
    Cancel,
}

/// Outbound intents produced by the reducer. The runner executes them.
#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    /// Send a ledgered mutation. `key` is stable for the user action.
    Ipc {
        slot: MutationSlot,
        command: String,
        payload: serde_json::Value,
        key: String,
    },
    /// Read-only request (never ledgered): snapshot / events / goal.list.
    Read {
        command: String,
        payload: serde_json::Value,
    },
    /// Re-fetch the snapshot (gap resync or post-mutation refresh).
    Resnapshot { goal_id: String },
    /// Long-poll `goal.events` for the attached goal from the cursor.
    StartEventStream { goal_id: String, after: i64 },
    /// Exit the TUI client. The goal continues in the Supervisor.
    Exit,
}
