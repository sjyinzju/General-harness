//! The TUI reducer — a pure `reduce(state, action) -> Vec<Effect>` fold.
//!
//! Invariants enforced here:
//! * The snapshot is the resync authority; events only fold forward from
//!   the snapshot cursor (`seq <= cursor` ignored, `cursor+1` applied,
//!   gap → resnapshot).
//! * Mutations are Effects naming IPC commands — the reducer never mutates
//!   business truth locally (pause/resume/cancel flip only via events or
//!   snapshots, never on key press).
//! * Every user mutation gets ONE stable idempotency key; retries reuse it.
//! * Conflicts are surfaced, never silently retried.

use harness_core::contracts::presentation::{GoalSnapshot, PendingInteraction, PresentationEvent};
use serde_json::json;

use super::action::{Effect, KeyIntent, MutationSlot, TuiAction};
use super::commands::{parse_command, Command};
use super::spec::build_interactive_goal_spec;
use super::state::{
    ClarifyQuestionUi, ConnectionStatus, ConversationEntry, Focus, InputMode, TuiAppState,
};

/// Upper bound for the local conversation buffer (drop-oldest).
const MAX_CONVERSATION_LINES: usize = 500;
/// Toast lifetime in ticks (~250ms each).
const TOAST_TICKS: u8 = 24;

/// Reduce one action into the new state, returning outbound effects.
pub fn reduce(state: &mut TuiAppState, action: TuiAction) -> Vec<Effect> {
    match action {
        TuiAction::Resize { cols, rows } => {
            state.term_cols = cols;
            state.term_rows = rows;
            Vec::new()
        }
        TuiAction::Tick => on_tick(state),
        TuiAction::Connected => on_connected(state),
        TuiAction::Disconnected { reason } => on_disconnected(state, &reason),
        TuiAction::GoalsListed { goals } => on_goals_listed(state, goals),
        TuiAction::SnapshotReceived { snapshot } => on_snapshot(state, snapshot),
        TuiAction::EventsReceived { events } => on_events(state, &events),
        TuiAction::MutationAcked { slot, payload } => on_mutation_acked(state, slot, payload),
        TuiAction::MutationFailed { slot, message } => {
            on_mutation_failed(state, slot, &message, false)
        }
        TuiAction::MutationConflict { slot, message } => {
            on_mutation_failed(state, slot, &message, true)
        }
        TuiAction::Notice { message } => {
            set_toast(state, &message);
            push_line(state, "Error", message);
            Vec::new()
        }
        TuiAction::Submit => on_submit(state),
        TuiAction::CharInput(c) => on_char(state, c),
        TuiAction::Key(k) => on_key(state, k),
        TuiAction::Quit => on_quit(state),
    }
}

// ── Lifecycle actions ──────────────────────────────────────────────────

fn on_tick(state: &mut TuiAppState) -> Vec<Effect> {
    if state.toast_ttl > 0 {
        state.toast_ttl -= 1;
        if state.toast_ttl == 0 {
            state.toast = None;
        }
    }
    // Dirty panels refresh from the snapshot authority (events only carry
    // interaction truth; task/activity detail comes from snapshots).
    if state.panels_dirty
        && !state.resync_needed
        && state.active_goal_id.is_some()
        && state.connection == ConnectionStatus::Connected
    {
        if let Some(goal_id) = state.active_goal_id.clone() {
            return vec![Effect::Resnapshot { goal_id }];
        }
    }
    Vec::new()
}

fn on_connected(state: &mut TuiAppState) -> Vec<Effect> {
    state.connection = ConnectionStatus::Connected;
    set_toast(state, "connected to supervisor");
    if let Some(goal_id) = state.active_goal_id.clone() {
        vec![Effect::Resnapshot { goal_id }]
    } else {
        vec![Effect::Read {
            command: "goal.list".to_string(),
            payload: json!({}),
        }]
    }
}

fn on_disconnected(state: &mut TuiAppState, reason: &str) -> Vec<Effect> {
    // First loss → reconnecting; repeated loss while already reconnecting
    // → shown as disconnected (the probe task keeps trying either way).
    state.connection = if state.connection == ConnectionStatus::Reconnecting {
        ConnectionStatus::Disconnected
    } else {
        ConnectionStatus::Reconnecting
    };
    set_toast(state, &format!("connection lost: {reason} — reconnecting"));
    push_line(
        state,
        "Event",
        format!("connection lost ({reason}) — reconnecting..."),
    );
    Vec::new()
}

fn on_goals_listed(state: &mut TuiAppState, goals: Vec<super::state::GoalListItem>) -> Vec<Effect> {
    state.goals = goals;
    if state.goals.is_empty() {
        push_line(
            state,
            "Harness",
            "no goals yet — type a goal and press Enter",
        );
        return Vec::new();
    }
    let mut summary = format!("{} goal(s):", state.goals.len());
    for g in state.goals.iter().take(5) {
        summary.push_str(&format!(
            "\n  {} [{}] {} ({})",
            g.goal_id, g.state, g.title, g.updated_at
        ));
    }
    push_line(state, "Harness", summary);

    // Auto-attach to the most recent goal when nothing is attached.
    if state.active_goal_id.is_none() {
        if let Some(first) = state.goals.first() {
            return attach_goal(state, first.goal_id.clone());
        }
    }
    Vec::new()
}

fn attach_goal(state: &mut TuiAppState, goal_id: String) -> Vec<Effect> {
    state.active_goal_id = Some(goal_id.clone());
    state.snapshot = None;
    state.cursor = 0;
    state.resync_needed = false;
    state.pending = Default::default();
    push_line(state, "Harness", format!("attaching to goal {goal_id}..."));
    vec![Effect::Resnapshot { goal_id }]
}

// ── Snapshot: the resync authority ─────────────────────────────────────

fn on_snapshot(state: &mut TuiAppState, snapshot: Box<GoalSnapshot>) -> Vec<Effect> {
    let goal_id = snapshot.goal.goal_id.clone();
    let new_state = snapshot.goal.state.clone();
    let first_attach = state
        .active_goal_id
        .as_deref()
        .map(|id| id != goal_id)
        .unwrap_or(true)
        || state.snapshot.is_none();
    let prev_state = state.goal_state().map(str::to_string);

    state.active_goal_id = Some(goal_id.clone());
    state.cursor = snapshot.last_event_sequence;
    state.resync_needed = false;
    state.panels_dirty = false;
    state.snapshot = Some(*snapshot);

    rebuild_pending_from_snapshot(state);
    recompute_input_mode(state);

    if first_attach {
        push_line(
            state,
            "Harness",
            format!(
                "attached: {} — \"{}\" [state: {}]",
                goal_id,
                state
                    .snapshot
                    .as_ref()
                    .map(|s| s.goal.title.clone())
                    .unwrap_or_default(),
                new_state
            ),
        );
    } else if prev_state.as_deref() != Some(new_state.as_str()) {
        push_line(state, "Event", format!("goal state: {new_state}"));
    }

    // Restart the event stream from the fresh cursor.
    vec![Effect::StartEventStream {
        goal_id,
        after: state.cursor,
    }]
}

/// Rebuild modal state from `pending_interactions`. Approvals bind exact
/// plan revisions (server-provided ids, never cached loosely).
fn rebuild_pending_from_snapshot(state: &mut TuiAppState) {
    let interactions: Vec<PendingInteraction> = state
        .snapshot
        .as_ref()
        .map(|s| s.pending_interactions.clone())
        .unwrap_or_default();

    // Close modals whose interaction no longer exists.
    let clarify_ids: Vec<&str> = interactions
        .iter()
        .filter(|i| i.kind == "provide_missing_information")
        .map(|i| i.approval_id.as_str())
        .collect();
    if let Some(id) = state.pending.clarify_approval_id.as_deref() {
        if !clarify_ids.contains(&id) {
            state.pending.clarify_approval_id = None;
            state.pending.clarify_questions.clear();
            state.pending.clarify_index = 0;
        }
    }
    let approve_ids: Vec<&str> = interactions
        .iter()
        .filter(|i| i.kind != "provide_missing_information")
        .map(|i| i.approval_id.as_str())
        .collect();
    if let Some(id) = state.pending.approve_approval_id.as_deref() {
        if !approve_ids.contains(&id) {
            state.pending.approve_approval_id = None;
            state.pending.approve_plan_revision_id = None;
            state.pending.approve_revision_number = None;
            state.pending.request_changes_mode = false;
        }
    }

    for interaction in &interactions {
        if interaction.kind == "provide_missing_information" {
            open_clarify(state, interaction);
        } else {
            open_approval(state, interaction);
        }
    }
}

fn open_clarify(state: &mut TuiAppState, interaction: &PendingInteraction) {
    // Preserve in-progress answers when the same interaction reappears.
    if state.pending.clarify_approval_id.as_deref() == Some(interaction.approval_id.as_str()) {
        return;
    }
    let questions = interaction
        .requested_action
        .get("questions")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let parsed: Vec<ClarifyQuestionUi> = questions
        .iter()
        .map(|q| ClarifyQuestionUi {
            question_id: q
                .get("question_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            prompt: q
                .get("prompt")
                .and_then(|v| v.as_str())
                .unwrap_or("(no prompt)")
                .to_string(),
            choices: q
                .get("choices")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|c| c.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default(),
            required: q.get("required").and_then(|v| v.as_bool()).unwrap_or(true),
            answer: String::new(),
        })
        .collect();
    state.pending.clarify_approval_id = Some(interaction.approval_id.clone());
    state.pending.clarify_questions = parsed;
    state.pending.clarify_index = 0;
    push_line(
        state,
        "Harness",
        format!("clarification needed: {}", interaction.reason),
    );
}

fn open_approval(state: &mut TuiAppState, interaction: &PendingInteraction) {
    if state.pending.approve_approval_id.as_deref() == Some(interaction.approval_id.as_str()) {
        return;
    }
    state.pending.approve_approval_id = Some(interaction.approval_id.clone());
    state.pending.approve_plan_revision_id = interaction.plan_revision_id.clone();
    state.pending.approve_revision_number = interaction
        .requested_action
        .get("revision_number")
        .and_then(|v| v.as_i64());
    state.pending.request_changes_mode = false;
    let task_count = interaction
        .requested_action
        .get("task_count")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    push_line(
        state,
        "Harness",
        format!(
            "plan revision {} awaits your approval ({} task(s)) — [a]pprove / [e]diting / re[x] reject",
            state.pending.approve_revision_number.unwrap_or(0),
            task_count
        ),
    );
}

// ── Event fold with cursor rules ───────────────────────────────────────

fn on_events(state: &mut TuiAppState, events: &[PresentationEvent]) -> Vec<Effect> {
    for ev in events {
        if ev.sequence <= state.cursor {
            continue; // duplicate / already folded
        }
        if ev.sequence > state.cursor + 1 {
            // GAP — the snapshot is the resync authority.
            state.resync_needed = true;
            if let Some(goal_id) = state.active_goal_id.clone() {
                return vec![Effect::Resnapshot { goal_id }];
            }
            return Vec::new();
        }
        state.cursor = ev.sequence;
        apply_event(state, ev);
    }
    Vec::new()
}

fn apply_event(state: &mut TuiAppState, ev: &PresentationEvent) {
    let p = &ev.payload;
    // Event lines carry the server timestamp for the conversation panel.
    let line = |s: &mut TuiAppState, text: String| {
        s.conversation
            .push(ConversationEntry::new("Event", text).with_at(ev.occurred_at.clone()));
        if s.conversation.len() > MAX_CONVERSATION_LINES {
            let drop = s.conversation.len() - MAX_CONVERSATION_LINES;
            s.conversation.drain(..drop);
        }
    };
    match ev.event_type.as_str() {
        "goal_state_changed" => {
            let to = p.get("to").and_then(|v| v.as_str()).unwrap_or("?");
            if let Some(snap) = state.snapshot.as_mut() {
                snap.goal.state = to.to_string();
            }
            line(state, format!("goal state: {to}"));
        }
        "clarification_requested" => {
            line(state, "harness asks for clarification".to_string());
        }
        "clarification_answered" => {
            let id = p.get("approval_id").and_then(|v| v.as_str()).unwrap_or("");
            if state.pending.clarify_approval_id.as_deref() == Some(id) {
                state.pending.clarify_approval_id = None;
                state.pending.clarify_questions.clear();
                state.pending.clarify_index = 0;
            }
            line(state, "clarification answered".to_string());
        }
        "plan_approval_requested" => {
            let n = p
                .get("revision_number")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            line(state, format!("plan revision {n} awaiting approval"));
        }
        "plan_approved" => {
            close_approval(state);
            line(state, "plan approved — activating".to_string());
        }
        "plan_changes_requested" => {
            close_approval(state);
            line(state, "plan changes requested — replanning".to_string());
        }
        "plan_rejected" => {
            close_approval(state);
            line(state, "plan rejected".to_string());
        }
        "user_intervention_recorded" => {
            let cls = p
                .get("classification")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            line(state, format!("intervention recorded ({cls})"));
        }
        "goal_paused" => line(state, "goal paused".to_string()),
        "goal_resumed" => line(state, "goal resumed".to_string()),
        "goal_cancelled" => line(state, "goal cancelled".to_string()),
        "goal_succeeded" => line(state, "goal succeeded".to_string()),
        "goal_failed" => line(state, "goal failed".to_string()),
        "plan_activated" => line(state, "plan activated".to_string()),
        _ => { /* unknown event types advance the cursor but stay silent */ }
    }
    state.panels_dirty = true;
    recompute_input_mode(state);
}

fn close_approval(state: &mut TuiAppState) {
    state.pending.approve_approval_id = None;
    state.pending.approve_plan_revision_id = None;
    state.pending.approve_revision_number = None;
    state.pending.request_changes_mode = false;
}

// ── Mutation results ───────────────────────────────────────────────────

fn on_mutation_acked(
    state: &mut TuiAppState,
    slot: MutationSlot,
    payload: serde_json::Value,
) -> Vec<Effect> {
    let goal_id = state.active_goal_id.clone();
    match slot {
        MutationSlot::GoalCreate => {
            let id = payload
                .get("goal_id")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            push_line(state, "Harness", format!("goal created: {id}"));
        }
        MutationSlot::GoalStart => {
            push_line(
                state,
                "Harness",
                "goal started — planning the first revision",
            );
        }
        MutationSlot::Answer => {
            push_line(state, "Harness", "answers recorded");
        }
        MutationSlot::Approve => {
            close_approval(state);
            push_line(state, "Harness", "plan approved");
        }
        MutationSlot::RequestChanges => {
            close_approval(state);
            push_line(state, "Harness", "change feedback sent — replanning");
        }
        MutationSlot::Reject => {
            close_approval(state);
            push_line(state, "Harness", "plan rejected");
        }
        MutationSlot::Intervene => {}
        MutationSlot::Pause => push_line(state, "Harness", "pause requested"),
        MutationSlot::Resume => push_line(state, "Harness", "resume requested"),
        MutationSlot::Cancel => {
            state.pending.cancel_confirm = false;
            push_line(state, "Harness", "cancel requested");
        }
    }
    recompute_input_mode(state);
    // Refresh panels from the authority after any accepted mutation.
    goal_id
        .map(|id| vec![Effect::Resnapshot { goal_id: id }])
        .unwrap_or_default()
}

fn on_mutation_failed(
    state: &mut TuiAppState,
    slot: MutationSlot,
    message: &str,
    conflict: bool,
) -> Vec<Effect> {
    let label = if conflict { "CONFLICT" } else { "error" };
    let slot_name = format!("{slot:?}");
    set_toast(state, &format!("{label}: {slot_name} — {message}"));
    push_line(state, "Error", format!("{slot_name} {label}: {message}"));
    if conflict {
        // Never silently retry a Conflict — the user must decide.
        if matches!(slot, MutationSlot::Cancel) {
            state.pending.cancel_confirm = false;
        }
    }
    Vec::new()
}

// ── Submit routing ─────────────────────────────────────────────────────

fn on_submit(state: &mut TuiAppState) -> Vec<Effect> {
    let raw = state.input.take();
    let text = raw.trim().to_string();
    if text.is_empty() {
        // Empty submits are inert — except for required clarification
        // answers, where the user must see the "required" notice.
        if state.input_mode == InputMode::Answer {
            return submit_answer(state, &text);
        }
        return Vec::new();
    }

    if text.starts_with('/') {
        return match parse_command(&text) {
            Some(cmd) => handle_command(state, cmd),
            None => {
                set_toast(state, "unknown command — /help lists commands");
                push_line(state, "Error", format!("unknown command: {text}"));
                Vec::new()
            }
        };
    }

    match state.input_mode {
        InputMode::NewGoal | InputMode::Goal => submit_new_goal(state, &text),
        InputMode::Answer => submit_answer(state, &text),
        InputMode::PlanChanges => submit_plan_changes(state, &text),
        InputMode::Message => submit_intervention(state, &text),
    }
}

fn submit_new_goal(state: &mut TuiAppState, text: &str) -> Vec<Effect> {
    let spec = build_interactive_goal_spec(text, &state.repo_ctx);
    let goal_id = spec.goal_id.clone();
    let spec_json = match serde_json::to_value(&spec) {
        Ok(v) => v,
        Err(e) => {
            set_toast(state, &format!("failed to encode goal spec: {e}"));
            return Vec::new();
        }
    };
    push_line(state, "You", text);
    state.active_goal_id = Some(goal_id.clone());
    // goal.create is NOT ledgered — retry safety comes from the client-owned
    // goal_id (PK collision replays the original row).
    let create_key = next_key(state, "goal-create");
    let start_key = next_key(state, "goal-start");
    vec![
        Effect::Ipc {
            slot: MutationSlot::GoalCreate,
            command: "goal.create".to_string(),
            payload: json!({ "goal_spec": spec_json }),
            key: create_key,
        },
        Effect::Ipc {
            slot: MutationSlot::GoalStart,
            command: "goal.start".to_string(),
            payload: json!({ "goal_id": goal_id }),
            key: start_key,
        },
    ]
}

fn submit_answer(state: &mut TuiAppState, text: &str) -> Vec<Effect> {
    let count = state.pending.clarify_questions.len();
    let idx = state.pending.clarify_index;
    if state.pending.clarify_approval_id.is_none() || idx >= count {
        // Modal gone — fall back to an intervention if a goal is attached.
        return submit_intervention(state, text);
    }
    let required = state.pending.clarify_questions[idx].required;
    if text.is_empty() && required {
        set_toast(state, "this question requires an answer");
        return Vec::new();
    }
    state.pending.clarify_questions[idx].answer = text.to_string();
    push_line(state, "You", text);
    state.pending.clarify_index += 1;

    if state.pending.clarify_index >= count {
        let approval_id = state
            .pending
            .clarify_approval_id
            .clone()
            .unwrap_or_default();
        // Map keyed by question_id — the shape the I8A contract uses.
        let mut answers = serde_json::Map::new();
        for q in &state.pending.clarify_questions {
            answers.insert(q.question_id.clone(), json!(q.answer));
        }
        let key = next_key(state, "answer");
        vec![Effect::Ipc {
            slot: MutationSlot::Answer,
            command: "goal.answer".to_string(),
            payload: json!({ "approval_id": approval_id, "answers": answers }),
            key,
        }]
    } else {
        Vec::new()
    }
}

fn submit_plan_changes(state: &mut TuiAppState, text: &str) -> Vec<Effect> {
    let approval_id = match state.pending.approve_approval_id.clone() {
        Some(id) => id,
        None => return submit_intervention(state, text),
    };
    push_line(state, "You", format!("[plan changes] {text}"));
    state.pending.request_changes_mode = false;
    let key = next_key(state, "request-changes");
    vec![Effect::Ipc {
        slot: MutationSlot::RequestChanges,
        command: "goal.request_changes".to_string(),
        payload: json!({ "approval_id": approval_id, "feedback": text }),
        key,
    }]
}

fn submit_intervention(state: &mut TuiAppState, text: &str) -> Vec<Effect> {
    let goal_id = match state.active_goal_id.clone() {
        Some(id) => id,
        None => {
            set_toast(state, "no goal attached — text is submitted as a new goal");
            return submit_new_goal(state, text);
        }
    };
    if state.goal_is_terminal() {
        set_toast(
            state,
            "goal already finished — text is submitted as a new goal",
        );
        return submit_new_goal(state, text);
    }
    push_line(state, "You", text);
    let key = next_key(state, "intervene");
    vec![Effect::Ipc {
        slot: MutationSlot::Intervene,
        command: "goal.intervene".to_string(),
        payload: json!({ "goal_id": goal_id, "message": text }),
        key,
    }]
}

// ── Slash commands ─────────────────────────────────────────────────────

fn handle_command(state: &mut TuiAppState, cmd: Command) -> Vec<Effect> {
    match cmd {
        Command::Help => {
            state.pending.help_open = true;
            Vec::new()
        }
        Command::Clear => {
            state.conversation.clear();
            Vec::new()
        }
        Command::Quit => on_quit(state),
        Command::Goals => vec![Effect::Read {
            command: "goal.list".to_string(),
            payload: json!({}),
        }],
        Command::Goal(id) => attach_goal(state, id),
        Command::Pause => goal_mutation(state, MutationSlot::Pause, "goal.pause", "pause"),
        Command::Resume => goal_mutation(state, MutationSlot::Resume, "goal.resume", "resume"),
        Command::Cancel => {
            if state.active_goal_id.is_none() {
                set_toast(state, "no goal attached");
                return Vec::new();
            }
            if state.goal_is_terminal() {
                set_toast(state, "goal already finished");
                return Vec::new();
            }
            state.pending.cancel_confirm = true;
            Vec::new()
        }
        Command::Plan => {
            let text = match state.snapshot.as_ref() {
                None => "no snapshot yet".to_string(),
                Some(snap) => {
                    let plan = snap
                        .active_plan
                        .as_ref()
                        .or(snap.latest_plan.as_ref())
                        .map(|p| format!("revision {} [{}]", p.revision_number, p.state))
                        .unwrap_or_else(|| "no plan yet".to_string());
                    let mut out = format!("plan: {plan}\ntasks:");
                    for t in &snap.tasks {
                        out.push_str(&format!(
                            "\n  {} {} — {} [{}]",
                            super::state::task_symbol(&t.state),
                            t.client_ref,
                            t.title,
                            t.state
                        ));
                    }
                    out
                }
            };
            push_line(state, "Harness", text);
            Vec::new()
        }
        Command::Status => {
            let text = match state.snapshot.as_ref() {
                None => "no goal attached".to_string(),
                Some(snap) => format!(
                    "goal {} [{}]\nstate: {}\nplan: {}\ncursor: {}\nconnection: {}",
                    snap.goal.goal_id,
                    snap.goal.title,
                    snap.goal.state,
                    snap.active_plan
                        .as_ref()
                        .map(|p| format!("revision {}", p.revision_number))
                        .unwrap_or_else(|| "none".to_string()),
                    state.cursor,
                    state.connection.label()
                ),
            };
            push_line(state, "Harness", text);
            Vec::new()
        }
        Command::Usage => {
            let text = match state.snapshot.as_ref() {
                None => "no goal attached".to_string(),
                Some(snap) => {
                    if !snap.usage.usage_known {
                        "usage: not reported yet — nothing to show (numbers are never fabricated)"
                            .to_string()
                    } else {
                        let t = &snap.usage.totals;
                        format!(
                            "usage: input={} output={} cached={} tool_calls={} wall_ms={} cost_micros={}",
                            fmt_opt(t.input_tokens),
                            fmt_opt(t.output_tokens),
                            fmt_opt(t.cached_input_tokens),
                            fmt_opt(t.tool_calls),
                            fmt_opt(t.wall_time_ms),
                            fmt_opt(t.estimated_cost_micros),
                        )
                    }
                }
            };
            push_line(state, "Harness", text);
            Vec::new()
        }
    }
}

fn fmt_opt(v: Option<i64>) -> String {
    v.map(|n| n.to_string()).unwrap_or_else(|| "—".to_string())
}

fn goal_mutation(
    state: &mut TuiAppState,
    slot: MutationSlot,
    command: &str,
    verb: &str,
) -> Vec<Effect> {
    let goal_id = match state.active_goal_id.clone() {
        Some(id) => id,
        None => {
            set_toast(state, "no goal attached");
            return Vec::new();
        }
    };
    if state.goal_is_terminal() {
        set_toast(state, "goal already finished");
        return Vec::new();
    }
    let key = next_key(state, &format!("{slot:?}"));
    push_line(state, "You", format!("/{verb}"));
    vec![Effect::Ipc {
        slot,
        command: command.to_string(),
        payload: json!({ "goal_id": goal_id }),
        key,
    }]
}

fn on_quit(state: &mut TuiAppState) -> Vec<Effect> {
    state.exit_requested = true;
    push_line(
        state,
        "Harness",
        "exiting TUI — the goal KEEPS RUNNING in the Supervisor",
    );
    vec![Effect::Exit]
}

// ── Keys ───────────────────────────────────────────────────────────────

fn on_char(state: &mut TuiAppState, c: char) -> Vec<Effect> {
    if state.pending.help_open {
        if c == 'q' || c == 'Q' {
            state.pending.help_open = false;
        }
        return Vec::new();
    }
    if state.pending.cancel_confirm {
        return match c {
            'y' | 'Y' => confirm_cancel(state),
            'n' | 'N' => {
                state.pending.cancel_confirm = false;
                Vec::new()
            }
            _ => Vec::new(),
        };
    }
    if state.pending.approve_approval_id.is_some() && !state.pending.request_changes_mode {
        return match c {
            'a' | 'A' => approve_effect(state),
            'e' | 'E' => {
                state.pending.request_changes_mode = true;
                recompute_input_mode(state);
                Vec::new()
            }
            'x' | 'X' => reject_effect(state),
            _ => Vec::new(),
        };
    }
    // Clarify modal and normal editing both type into the input buffer.
    state.input.insert(c);
    Vec::new()
}

fn on_key(state: &mut TuiAppState, k: KeyIntent) -> Vec<Effect> {
    if state.pending.help_open {
        if matches!(k, KeyIntent::Esc) {
            state.pending.help_open = false;
        }
        return Vec::new();
    }
    if state.pending.cancel_confirm {
        return match k {
            KeyIntent::Esc => {
                state.pending.cancel_confirm = false;
                Vec::new()
            }
            _ => Vec::new(),
        };
    }
    if state.pending.approve_approval_id.is_some() && !state.pending.request_changes_mode {
        return match k {
            KeyIntent::Esc => {
                set_toast(
                    state,
                    "decision required: [a]pprove / [e] request changes / re[x] reject",
                );
                Vec::new()
            }
            _ => Vec::new(),
        };
    }
    if state.pending.clarify_approval_id.is_some() && matches!(k, KeyIntent::Esc) {
        set_toast(
            state,
            "answer required — or /quit to leave (the goal keeps waiting)",
        );
        return Vec::new();
    }
    // While any modal is open, panel navigation keys stay inert — the user
    // is mid-decision, not browsing panels.
    if state.pending.any_modal()
        && matches!(
            k,
            KeyIntent::Up
                | KeyIntent::Down
                | KeyIntent::PageUp
                | KeyIntent::PageDown
                | KeyIntent::Tab
        )
    {
        return Vec::new();
    }
    handle_edit_key(state, k)
}

fn handle_edit_key(state: &mut TuiAppState, k: KeyIntent) -> Vec<Effect> {
    match k {
        KeyIntent::Backspace => state.input.backspace(),
        KeyIntent::Delete => state.input.delete(),
        KeyIntent::Left => state.input.move_left(),
        KeyIntent::Right => state.input.move_right(),
        KeyIntent::Home => state.input.move_home(),
        KeyIntent::End => state.input.move_end(),
        KeyIntent::ClearLine => state.input.clear(),
        KeyIntent::Esc => state.input.clear(),
        KeyIntent::Tab => {
            state.focus = match state.focus {
                Focus::Conversation => Focus::Tasks,
                Focus::Tasks => Focus::Activity,
                Focus::Activity => Focus::Conversation,
            };
        }
        KeyIntent::Up => scroll(state, -1),
        KeyIntent::Down => scroll(state, 1),
        KeyIntent::PageUp => {
            state.conversation_scroll = state.conversation_scroll.saturating_add(10)
        }
        KeyIntent::PageDown => {
            state.conversation_scroll = state.conversation_scroll.saturating_sub(10)
        }
    }
    Vec::new()
}

fn scroll(state: &mut TuiAppState, delta: i32) {
    let apply = |v: usize| -> usize {
        if delta < 0 {
            v.saturating_sub((-delta) as usize)
        } else {
            v.saturating_add(delta as usize)
        }
    };
    match state.focus {
        Focus::Tasks => state.tasks_scroll = apply(state.tasks_scroll),
        Focus::Activity => state.activity_scroll = apply(state.activity_scroll),
        Focus::Conversation => {
            if delta < 0 {
                state.conversation_scroll = state.conversation_scroll.saturating_add(1);
            } else {
                state.conversation_scroll = state.conversation_scroll.saturating_sub(1);
            }
        }
    }
}

fn confirm_cancel(state: &mut TuiAppState) -> Vec<Effect> {
    let goal_id = match state.active_goal_id.clone() {
        Some(id) => id,
        None => {
            state.pending.cancel_confirm = false;
            return Vec::new();
        }
    };
    state.pending.cancel_confirm = false;
    let key = next_key(state, "Cancel");
    push_line(state, "You", "/cancel (confirmed)");
    vec![Effect::Ipc {
        slot: MutationSlot::Cancel,
        command: "goal.cancel".to_string(),
        payload: json!({ "goal_id": goal_id }),
        key,
    }]
}

fn approve_effect(state: &mut TuiAppState) -> Vec<Effect> {
    let approval_id = match state.pending.approve_approval_id.clone() {
        Some(id) => id,
        None => return Vec::new(),
    };
    let mut payload = json!({ "approval_id": approval_id });
    // Approvals bind exact plan revisions — forward the server-provided id.
    if let Some(rev) = state.pending.approve_plan_revision_id.clone() {
        payload["expected_plan_revision_id"] = json!(rev);
    }
    push_line(state, "You", "[approve plan]");
    let key = next_key(state, "Approve");
    vec![Effect::Ipc {
        slot: MutationSlot::Approve,
        command: "goal.approve".to_string(),
        payload,
        key,
    }]
}

fn reject_effect(state: &mut TuiAppState) -> Vec<Effect> {
    let approval_id = match state.pending.approve_approval_id.clone() {
        Some(id) => id,
        None => return Vec::new(),
    };
    let mut payload = json!({ "approval_id": approval_id });
    if let Some(rev) = state.pending.approve_plan_revision_id.clone() {
        payload["expected_plan_revision_id"] = json!(rev);
    }
    payload["reason"] = json!("rejected from the TUI console");
    push_line(state, "You", "[reject plan]");
    let key = next_key(state, "Reject");
    vec![Effect::Ipc {
        slot: MutationSlot::Reject,
        command: "goal.reject".to_string(),
        payload,
        key,
    }]
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Stable idempotency key for one user action; retries reuse it.
fn next_key(state: &mut TuiAppState, label: &str) -> String {
    state.local_seq += 1;
    format!("tui-{}-{}", label, state.local_seq)
}

fn set_toast(state: &mut TuiAppState, text: &str) {
    state.toast = Some(text.to_string());
    state.toast_ttl = TOAST_TICKS;
}

fn push_line(state: &mut TuiAppState, role: &str, text: impl Into<String>) {
    state.conversation.push(ConversationEntry::new(role, text));
    if state.conversation.len() > MAX_CONVERSATION_LINES {
        let drop = state.conversation.len() - MAX_CONVERSATION_LINES;
        state.conversation.drain(..drop);
    }
}

fn recompute_input_mode(state: &mut TuiAppState) {
    state.input_mode = if state.pending.clarify_approval_id.is_some() {
        InputMode::Answer
    } else if state.pending.approve_approval_id.is_some() && state.pending.request_changes_mode {
        InputMode::PlanChanges
    } else if state.active_goal_id.is_none() || state.snapshot.is_none() {
        InputMode::NewGoal
    } else if state.goal_is_terminal() {
        InputMode::Goal
    } else {
        InputMode::Message
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_core::contracts::presentation::{
        RunningActivity, SnapshotGoal, SnapshotPlan, UsageSummary,
    };

    fn snap_with_state(goal_state: &str) -> GoalSnapshot {
        GoalSnapshot {
            goal: SnapshotGoal {
                goal_id: "goal-1".into(),
                revision: 1,
                title: "Test goal".into(),
                objective: "Do the thing".into(),
                state: goal_state.into(),
                budget: json!({}),
                approval_policy: json!({}),
                created_at: "2026-01-01T00:00:00Z".into(),
                updated_at: "2026-01-01T00:00:00Z".into(),
            },
            active_plan: Some(SnapshotPlan {
                plan_revision_id: "pr-1".into(),
                revision_number: 1,
                state: "active".into(),
            }),
            latest_plan: None,
            tasks: Vec::new(),
            pending_interactions: Vec::new(),
            interventions: Vec::new(),
            running_activities: vec![RunningActivity {
                run_id: "run-1".into(),
                state: "running".into(),
                iteration_number: 1,
                plan_revision_id: Some("pr-1".into()),
                task_title: None,
                agent_kind: None,
                model: None,
            }],
            usage: UsageSummary::default(),
            last_event_sequence: 5,
        }
    }

    fn snap_box(state: &str) -> Box<GoalSnapshot> {
        Box::new(snap_with_state(state))
    }

    fn ev(seq: i64, event_type: &str, payload: serde_json::Value) -> PresentationEvent {
        PresentationEvent {
            sequence: seq,
            goal_id: "goal-1".into(),
            event_type: event_type.into(),
            occurred_at: "2026-01-01T00:00:01Z".into(),
            payload,
        }
    }

    fn attached_active() -> TuiAppState {
        let mut s = TuiAppState::new("demo");
        let effects = reduce(
            &mut s,
            TuiAction::SnapshotReceived {
                snapshot: snap_box("active"),
            },
        );
        assert!(matches!(
            effects[0],
            Effect::StartEventStream { after: 5, .. }
        ));
        s
    }

    #[test]
    fn snapshot_sets_cursor_and_starts_stream() {
        let mut s = TuiAppState::new("demo");
        let effects = reduce(
            &mut s,
            TuiAction::SnapshotReceived {
                snapshot: snap_box("planning"),
            },
        );
        assert_eq!(s.cursor, 5);
        assert_eq!(s.active_goal_id.as_deref(), Some("goal-1"));
        assert_eq!(s.input_mode, InputMode::Message);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::StartEventStream { goal_id, after: 5 } if goal_id == "goal-1"
        ));
    }

    #[test]
    fn event_cursor_rules_duplicate_next_and_gap() {
        let mut s = attached_active();
        // duplicate → ignored
        let fx = reduce(
            &mut s,
            TuiAction::EventsReceived {
                events: vec![ev(5, "goal_paused", json!({}))],
            },
        );
        assert!(fx.is_empty());
        assert_eq!(s.cursor, 5);
        // next → applied
        let fx = reduce(
            &mut s,
            TuiAction::EventsReceived {
                events: vec![ev(6, "goal_paused", json!({}))],
            },
        );
        assert!(fx.is_empty());
        assert_eq!(s.cursor, 6);
        assert!(s.panels_dirty);
        // gap → resnapshot effect
        let fx = reduce(
            &mut s,
            TuiAction::EventsReceived {
                events: vec![ev(9, "goal_resumed", json!({}))],
            },
        );
        assert!(s.resync_needed);
        assert!(matches!(fx[0], Effect::Resnapshot { .. }));
    }

    #[test]
    fn goal_state_changed_updates_projection() {
        let mut s = attached_active();
        reduce(
            &mut s,
            TuiAction::EventsReceived {
                events: vec![ev(
                    6,
                    "goal_state_changed",
                    json!({"from":"active","to":"paused"}),
                )],
            },
        );
        assert_eq!(s.goal_state(), Some("paused"));
    }

    #[test]
    fn submit_new_goal_emits_create_then_start_with_stable_keys() {
        let mut s = TuiAppState::new("demo");
        s.connection = ConnectionStatus::Connected;
        for c in "make it work".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        let fx = reduce(&mut s, TuiAction::Submit);
        assert_eq!(fx.len(), 2);
        match (&fx[0], &fx[1]) {
            (
                Effect::Ipc {
                    slot: MutationSlot::GoalCreate,
                    command,
                    key,
                    ..
                },
                Effect::Ipc {
                    slot: MutationSlot::GoalStart,
                    key: key2,
                    ..
                },
            ) => {
                assert_eq!(command, "goal.create");
                assert_ne!(key, key2);
            }
            _ => panic!("expected create+start effects"),
        }
        assert_eq!(s.input_mode, InputMode::NewGoal);
        assert!(s.active_goal_id.is_some());
        // Input buffer cleared after submit.
        assert!(s.input.is_empty());
    }

    #[test]
    fn slash_pause_emits_ledgered_effect_only_when_attached() {
        let mut s = TuiAppState::new("demo");
        for c in "/pause".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        let fx = reduce(&mut s, TuiAction::Submit);
        assert!(fx.is_empty()); // no goal attached

        let mut s = attached_active();
        for c in "/pause".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        let fx = reduce(&mut s, TuiAction::Submit);
        assert_eq!(fx.len(), 1);
        assert!(matches!(
            &fx[0],
            Effect::Ipc { slot: MutationSlot::Pause, command, payload, .. }
                if command == "goal.pause" && payload["goal_id"] == "goal-1"
        ));
    }

    #[test]
    fn cancel_requires_confirmation() {
        let mut s = attached_active();
        for c in "/cancel".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        let fx = reduce(&mut s, TuiAction::Submit);
        assert!(fx.is_empty());
        assert!(s.pending.cancel_confirm);
        // 'n' dismisses
        let fx = reduce(&mut s, TuiAction::CharInput('n'));
        assert!(fx.is_empty());
        assert!(!s.pending.cancel_confirm);
        // 'y' confirms → IPC effect
        reduce(&mut s, TuiAction::Submit); // reopen dialog: input empty, need retype
        for c in "/cancel".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        reduce(&mut s, TuiAction::Submit);
        let fx = reduce(&mut s, TuiAction::CharInput('y'));
        assert_eq!(fx.len(), 1);
        assert!(matches!(
            &fx[0],
            Effect::Ipc { slot: MutationSlot::Cancel, command, .. } if command == "goal.cancel"
        ));
    }

    #[test]
    fn quit_never_cancels_the_goal() {
        let mut s = attached_active();
        let fx = reduce(&mut s, TuiAction::Quit);
        assert_eq!(fx.len(), 1);
        assert!(matches!(fx[0], Effect::Exit));
        assert!(s.exit_requested);
        // No goal.cancel effect was ever emitted.
        assert!(fx.iter().all(|e| !matches!(
            e,
            Effect::Ipc {
                slot: MutationSlot::Cancel,
                ..
            }
        )));
    }

    #[test]
    fn snapshot_opens_clarify_modal_and_answer_flow_collects_then_sends() {
        let mut s = TuiAppState::new("demo");
        let mut snap = snap_with_state("active");
        snap.pending_interactions = vec![PendingInteraction {
            approval_id: "ap-1".into(),
            kind: "provide_missing_information".into(),
            plan_revision_id: None,
            reason: "missing info".into(),
            requested_action: json!({"questions":[
                {"question_id":"q1","prompt":"Which DB?","choices":["sqlite","pg"],"required":true},
                {"question_id":"q2","prompt":"Notes?","choices":[],"required":false}
            ]}),
            created_at: "2026-01-01T00:00:00Z".into(),
        }];
        reduce(
            &mut s,
            TuiAction::SnapshotReceived {
                snapshot: Box::new(snap),
            },
        );
        assert_eq!(s.input_mode, InputMode::Answer);
        assert_eq!(s.pending.clarify_questions.len(), 2);

        // First answer: buffered locally, nothing sent yet.
        for c in "sqlite".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        let fx = reduce(&mut s, TuiAction::Submit);
        assert!(fx.is_empty());
        assert_eq!(s.pending.clarify_index, 1);

        // Second answer: all collected → one goal.answer effect.
        for c in "none".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        let fx = reduce(&mut s, TuiAction::Submit);
        assert_eq!(fx.len(), 1);
        match &fx[0] {
            Effect::Ipc {
                slot: MutationSlot::Answer,
                command,
                payload,
                ..
            } => {
                assert_eq!(command, "goal.answer");
                assert_eq!(payload["approval_id"], "ap-1");
                assert_eq!(payload["answers"]["q1"], "sqlite");
                assert_eq!(payload["answers"]["q2"], "none");
            }
            _ => panic!("expected goal.answer"),
        }
    }

    #[test]
    fn required_answer_cannot_be_empty() {
        let mut s = TuiAppState::new("demo");
        let mut snap = snap_with_state("active");
        snap.pending_interactions = vec![PendingInteraction {
            approval_id: "ap-1".into(),
            kind: "provide_missing_information".into(),
            plan_revision_id: None,
            reason: "missing info".into(),
            requested_action: json!({"questions":[
                {"question_id":"q1","prompt":"Which DB?","choices":[],"required":true}
            ]}),
            created_at: "2026-01-01T00:00:00Z".into(),
        }];
        reduce(
            &mut s,
            TuiAction::SnapshotReceived {
                snapshot: Box::new(snap),
            },
        );
        let fx = reduce(&mut s, TuiAction::Submit);
        assert!(fx.is_empty());
        assert_eq!(s.pending.clarify_index, 0);
        assert!(s.toast.is_some());
    }

    #[test]
    fn plan_approval_modal_approve_binds_revision() {
        let mut s = TuiAppState::new("demo");
        let mut snap = snap_with_state("waiting_for_approval");
        snap.pending_interactions = vec![PendingInteraction {
            approval_id: "ap-9".into(),
            kind: "approve_initial_plan".into(),
            plan_revision_id: Some("pr-7".into()),
            reason: "initial plan".into(),
            requested_action: json!({"revision_number":1,"task_count":3}),
            created_at: "2026-01-01T00:00:00Z".into(),
        }];
        reduce(
            &mut s,
            TuiAction::SnapshotReceived {
                snapshot: Box::new(snap),
            },
        );
        assert_eq!(s.pending.approve_approval_id.as_deref(), Some("ap-9"));

        // 'a' approves with the bound revision id.
        let fx = reduce(&mut s, TuiAction::CharInput('a'));
        assert_eq!(fx.len(), 1);
        match &fx[0] {
            Effect::Ipc {
                slot: MutationSlot::Approve,
                command,
                payload,
                ..
            } => {
                assert_eq!(command, "goal.approve");
                assert_eq!(payload["approval_id"], "ap-9");
                assert_eq!(payload["expected_plan_revision_id"], "pr-7");
            }
            _ => panic!("expected goal.approve"),
        }
    }

    #[test]
    fn plan_approval_request_changes_switches_input_mode() {
        let mut s = TuiAppState::new("demo");
        let mut snap = snap_with_state("waiting_for_approval");
        snap.pending_interactions = vec![PendingInteraction {
            approval_id: "ap-9".into(),
            kind: "approve_initial_plan".into(),
            plan_revision_id: Some("pr-7".into()),
            reason: "initial plan".into(),
            requested_action: json!({"revision_number":1,"task_count":3}),
            created_at: "2026-01-01T00:00:00Z".into(),
        }];
        reduce(
            &mut s,
            TuiAction::SnapshotReceived {
                snapshot: Box::new(snap),
            },
        );
        let fx = reduce(&mut s, TuiAction::CharInput('e'));
        assert!(fx.is_empty());
        assert_eq!(s.input_mode, InputMode::PlanChanges);
        for c in "split task 2".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        let fx = reduce(&mut s, TuiAction::Submit);
        assert_eq!(fx.len(), 1);
        match &fx[0] {
            Effect::Ipc {
                slot: MutationSlot::RequestChanges,
                command,
                payload,
                ..
            } => {
                assert_eq!(command, "goal.request_changes");
                assert_eq!(payload["approval_id"], "ap-9");
                assert_eq!(payload["feedback"], "split task 2");
            }
            _ => panic!("expected goal.request_changes"),
        }
    }

    #[test]
    fn intervention_sent_only_for_active_goal() {
        let mut s = attached_active();
        for c in "please hurry".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        let fx = reduce(&mut s, TuiAction::Submit);
        assert_eq!(fx.len(), 1);
        assert!(matches!(
            &fx[0],
            Effect::Ipc { slot: MutationSlot::Intervene, command, payload, .. }
                if command == "goal.intervene" && payload["message"] == "please hurry"
        ));
    }

    #[test]
    fn terminal_goal_accepts_new_goal_text() {
        let mut s = TuiAppState::new("demo");
        reduce(
            &mut s,
            TuiAction::SnapshotReceived {
                snapshot: snap_box("succeeded"),
            },
        );
        assert_eq!(s.input_mode, InputMode::Goal);
        for c in "next thing".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        let fx = reduce(&mut s, TuiAction::Submit);
        assert_eq!(fx.len(), 2);
        assert!(matches!(
            &fx[0],
            Effect::Ipc {
                slot: MutationSlot::GoalCreate,
                ..
            }
        ));
    }

    #[test]
    fn mutation_conflict_is_surfaced_never_retried() {
        let mut s = attached_active();
        let fx = reduce(
            &mut s,
            TuiAction::MutationConflict {
                slot: MutationSlot::Pause,
                message: "key reuse with different payload".into(),
            },
        );
        assert!(fx.is_empty()); // no retry effect
        assert!(s.toast.as_deref().unwrap().contains("CONFLICT"));
        assert!(s
            .conversation
            .iter()
            .any(|e| e.role == "Error" && e.text.contains("CONFLICT")));
    }

    #[test]
    fn disconnect_marks_reconnecting_and_connect_resyncs() {
        let mut s = attached_active();
        let fx = reduce(
            &mut s,
            TuiAction::Disconnected {
                reason: "pipe closed".into(),
            },
        );
        assert!(fx.is_empty());
        assert_eq!(s.connection, ConnectionStatus::Reconnecting);
        let fx = reduce(&mut s, TuiAction::Connected);
        assert_eq!(fx.len(), 1);
        assert!(matches!(fx[0], Effect::Resnapshot { .. }));
        assert_eq!(s.connection, ConnectionStatus::Connected);
    }

    #[test]
    fn goals_listed_auto_attaches_most_recent() {
        let mut s = TuiAppState::new("demo");
        let fx = reduce(
            &mut s,
            TuiAction::GoalsListed {
                goals: vec![super::super::state::GoalListItem {
                    goal_id: "goal-9".into(),
                    title: "latest".into(),
                    state: "active".into(),
                    created_at: "t".into(),
                    updated_at: "t2".into(),
                }],
            },
        );
        assert_eq!(s.active_goal_id.as_deref(), Some("goal-9"));
        assert!(matches!(fx[0], Effect::Resnapshot { .. }));
    }

    #[test]
    fn unknown_command_reports_error() {
        let mut s = TuiAppState::new("demo");
        for c in "/bogus".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        let fx = reduce(&mut s, TuiAction::Submit);
        assert!(fx.is_empty());
        assert!(s.toast.is_some());
    }

    #[test]
    fn usage_never_fabricates_numbers() {
        let mut s = attached_active();
        for c in "/usage".chars() {
            reduce(&mut s, TuiAction::CharInput(c));
        }
        reduce(&mut s, TuiAction::Submit);
        let last = s.conversation.last().unwrap();
        assert!(last.text.contains("never fabricated"));
    }

    #[test]
    fn resize_and_scroll_do_not_emit_effects() {
        let mut s = attached_active();
        assert!(reduce(
            &mut s,
            TuiAction::Resize {
                cols: 120,
                rows: 40
            }
        )
        .is_empty());
        assert_eq!(s.term_cols, 120);
        assert!(reduce(&mut s, TuiAction::Key(KeyIntent::Up)).is_empty());
        assert!(reduce(&mut s, TuiAction::Key(KeyIntent::Tab)).is_empty());
        assert_eq!(s.focus, Focus::Tasks);
    }

    #[test]
    fn tick_emits_resnapshot_when_panels_dirty() {
        let mut s = attached_active();
        s.connection = ConnectionStatus::Connected;
        s.panels_dirty = true;
        let fx = reduce(&mut s, TuiAction::Tick);
        assert!(matches!(fx[0], Effect::Resnapshot { .. }));
    }
}
