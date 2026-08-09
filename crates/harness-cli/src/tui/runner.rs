//! The TUI runner — owns the tokio event loop and executes reducer effects.
//!
//! Architecture: one mpsc of `RunnerMsg`s drives everything. The reducer
//! stays pure; all IPC happens in spawned tasks that report back through
//! the channel. Mutations serialize through a queue so `goal.create`
//! always precedes `goal.start`, and transport failures requeue the same
//! idempotency key + payload for retry (Request Ledger replay).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use serde_json::json;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

use super::action::{Effect, KeyIntent, MutationSlot, TuiAction};
use super::gateway::{
    parse_events, parse_goal_list, parse_snapshot, GatewayError, GatewayReply, TuiGateway,
};
use super::reducer::reduce;
use super::render;
use super::state::{ConnectionStatus, TuiAppState};
use super::terminal::TerminalGuard;
use super::TuiOptions;

/// A queued mutation: stable key + payload reused on every retry.
#[derive(Debug, Clone)]
struct PendingMutation {
    slot: MutationSlot,
    command: String,
    payload: serde_json::Value,
    key: String,
}

/// Messages flowing into the main loop.
enum RunnerMsg {
    Ui(TuiAction),
    /// A mutation finished (or the transport failed) — carries the job back
    /// so the loop can requeue it on transport errors. Boxed to keep the
    /// enum small on the hot Ui path.
    MutationDone(Box<(PendingMutation, Result<GatewayReply, GatewayError>)>),
}

/// Timeout for one mutation round-trip; on expiry we retry with the SAME
/// key — the Request Ledger replays instead of double-applying.
const MUTATION_TIMEOUT: Duration = Duration::from_secs(30);
/// Long-poll window for goal.events (server caps at 30s).
const EVENT_WAIT_MS: u64 = 10_000;
/// Reconnect probe interval.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(2);
/// UI tick.
const TICK_INTERVAL: Duration = Duration::from_millis(250);

/// Run the interactive console until the user exits.
pub async fn run<G: TuiGateway>(gateway: G, options: TuiOptions) -> Result<(), super::TuiError> {
    let (_guard, mut terminal) =
        TerminalGuard::enter().map_err(|e| super::TuiError::Terminal(e.to_string()))?;

    let mut state = TuiAppState::new(&options.project_label);
    if let Ok(size) = terminal.size() {
        state.term_cols = size.width;
        state.term_rows = size.height;
    }

    let (tx, mut rx) = unbounded_channel::<RunnerMsg>();
    let ui_tx = tx.clone();

    // Terminal input task (blocking crossterm read on its own thread).
    std::thread::spawn(move || terminal_event_loop(ui_tx));

    // UI tick task.
    let tick_tx = tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(TICK_INTERVAL);
        loop {
            interval.tick().await;
            if tick_tx.send(RunnerMsg::Ui(TuiAction::Tick)).is_err() {
                return;
            }
        }
    });

    // Initial connection probe — handled uniformly through the channel so
    // its effects (goal.list / resnapshot) are executed by the main loop.
    if gateway.ping().await {
        let _ = tx.send(RunnerMsg::Ui(TuiAction::Connected));
    } else {
        let _ = tx.send(RunnerMsg::Ui(TuiAction::Disconnected {
            reason: "supervisor not reachable".into(),
        }));
        start_reconnect(gateway.clone(), tx.clone());
    }

    let stream_gen = Arc::new(AtomicU64::new(0));
    let mut queue: VecDeque<PendingMutation> = VecDeque::new();
    let mut in_flight = false;
    let mut exiting = false;

    terminal
        .draw(|f| render::draw(f, &state))
        .map_err(|e| super::TuiError::Run(e.to_string()))?;

    while !exiting {
        let Some(msg) = rx.recv().await else { break };
        let mut ctx = LoopCtx::new(tx.clone());
        match msg {
            RunnerMsg::Ui(action) => {
                if state.exit_requested && !matches!(action, TuiAction::Resize { .. }) {
                    continue;
                }
                apply(&mut state, action, &gateway, &mut ctx);
            }
            RunnerMsg::MutationDone(done) => {
                let (job, result) = *done;
                in_flight = false;
                match result {
                    Ok(reply) => {
                        let action = match reply {
                            GatewayReply::Success(p) | GatewayReply::Duplicate(p) => {
                                TuiAction::MutationAcked {
                                    slot: job.slot,
                                    payload: p,
                                }
                            }
                            GatewayReply::Conflict(message) => TuiAction::MutationConflict {
                                slot: job.slot,
                                message,
                            },
                            GatewayReply::Failure(message) => {
                                if job.slot == MutationSlot::GoalCreate {
                                    // Drop the dependent goal.start.
                                    if queue
                                        .front()
                                        .map(|m| m.slot == MutationSlot::GoalStart)
                                        .unwrap_or(false)
                                    {
                                        queue.pop_front();
                                    }
                                }
                                TuiAction::MutationFailed {
                                    slot: job.slot,
                                    message,
                                }
                            }
                        };
                        apply(&mut state, action, &gateway, &mut ctx);
                    }
                    Err(GatewayError::Transport(reason)) => {
                        // Same key + payload requeued at the front.
                        queue.push_front(job);
                        apply(
                            &mut state,
                            TuiAction::Disconnected { reason },
                            &gateway,
                            &mut ctx,
                        );
                        start_reconnect(gateway.clone(), tx.clone());
                    }
                    Err(GatewayError::Serialization(reason)) => {
                        apply(
                            &mut state,
                            TuiAction::MutationFailed {
                                slot: job.slot,
                                message: reason,
                            },
                            &gateway,
                            &mut ctx,
                        );
                    }
                }
            }
        }

        // Drain effect outputs: ctx holds effects + stream requests.
        for effect in ctx.effects.drain(..) {
            match effect {
                Effect::Ipc {
                    slot,
                    command,
                    payload,
                    key,
                } => {
                    queue.push_back(PendingMutation {
                        slot,
                        command,
                        payload,
                        key,
                    });
                }
                Effect::Read { command, payload } => {
                    spawn_read(
                        gateway.clone(),
                        tx.clone(),
                        command,
                        payload,
                        stream_gen.clone(),
                    );
                }
                Effect::Resnapshot { goal_id } => {
                    spawn_snapshot(gateway.clone(), tx.clone(), goal_id);
                }
                Effect::StartEventStream { goal_id, after } => {
                    let gen = stream_gen.fetch_add(1, Ordering::SeqCst) + 1;
                    spawn_event_poll(
                        gateway.clone(),
                        tx.clone(),
                        goal_id,
                        after,
                        gen,
                        stream_gen.clone(),
                    );
                }
                Effect::Exit => exiting = true,
            }
        }
        if !in_flight {
            if let Some(job) = queue.pop_front() {
                if state.connection == ConnectionStatus::Connected {
                    in_flight = true;
                    spawn_mutation(gateway.clone(), tx.clone(), job);
                } else {
                    queue.push_front(job);
                }
            }
        }

        terminal
            .draw(|f| render::draw(f, &state))
            .map_err(|e| super::TuiError::Run(e.to_string()))?;
    }

    // Guard drop restores the terminal; tell the user the goal lives on.
    drop(terminal);
    println!("TUI closed — the goal continues running in the Supervisor.");
    Ok(())
}

/// Context threaded through `apply` to collect effects.
struct LoopCtx {
    effects: Vec<Effect>,
}

impl LoopCtx {
    fn new(_tx: UnboundedSender<RunnerMsg>) -> Self {
        Self {
            effects: Vec::new(),
        }
    }
}

fn apply<G: TuiGateway>(
    state: &mut TuiAppState,
    action: TuiAction,
    _gateway: &G,
    ctx: &mut LoopCtx,
) {
    ctx.effects.extend(reduce(state, action));
}

/// Serialize one mutation with a timeout; the job always returns through
/// `MutationDone` so the loop can requeue on transport trouble.
fn spawn_mutation<G: TuiGateway>(gateway: G, tx: UnboundedSender<RunnerMsg>, job: PendingMutation) {
    tokio::spawn(async move {
        let result = tokio::time::timeout(
            MUTATION_TIMEOUT,
            gateway.send(&job.command, job.payload.clone(), &job.key),
        )
        .await;
        let outcome = match result {
            Ok(inner) => inner,
            Err(_) => Err(GatewayError::Transport(
                "timeout waiting for supervisor".into(),
            )),
        };
        let _ = tx.send(RunnerMsg::MutationDone(Box::new((job, outcome))));
    });
}

fn spawn_read<G: TuiGateway>(
    gateway: G,
    tx: UnboundedSender<RunnerMsg>,
    command: String,
    payload: serde_json::Value,
    _stream_gen: Arc<AtomicU64>,
) {
    tokio::spawn(async move {
        let key = format!("tui-read-{}", uuid::Uuid::new_v4());
        match gateway.send(&command, payload, &key).await {
            Ok(reply) => {
                if command == "goal.list" {
                    if let Ok(goals) = parse_goal_list(reply) {
                        let _ = tx.send(RunnerMsg::Ui(TuiAction::GoalsListed { goals }));
                    }
                }
            }
            Err(GatewayError::Transport(reason)) => {
                let _ = tx.send(RunnerMsg::Ui(TuiAction::Disconnected { reason }));
            }
            Err(_) => {}
        }
    });
}

fn spawn_snapshot<G: TuiGateway>(gateway: G, tx: UnboundedSender<RunnerMsg>, goal_id: String) {
    tokio::spawn(async move {
        let key = format!("tui-snap-{}", uuid::Uuid::new_v4());
        match gateway
            .send("goal.snapshot", json!({ "goal_id": goal_id }), &key)
            .await
        {
            Ok(reply) => match parse_snapshot(reply) {
                Ok(snapshot) => {
                    let _ = tx.send(RunnerMsg::Ui(TuiAction::SnapshotReceived {
                        snapshot: Box::new(snapshot),
                    }));
                }
                Err(message) => {
                    let _ = tx.send(RunnerMsg::Ui(TuiAction::Notice { message }));
                }
            },
            Err(GatewayError::Transport(reason)) => {
                let _ = tx.send(RunnerMsg::Ui(TuiAction::Disconnected { reason }));
            }
            Err(_) => {}
        }
    });
}

/// Long-poll goal.events until a newer stream generation supersedes us.
fn spawn_event_poll<G: TuiGateway>(
    gateway: G,
    tx: UnboundedSender<RunnerMsg>,
    goal_id: String,
    after: i64,
    gen: u64,
    current: Arc<AtomicU64>,
) {
    tokio::spawn(async move {
        let mut after = after;
        loop {
            if current.load(Ordering::SeqCst) != gen {
                return;
            }
            let key = format!("tui-events-{}", uuid::Uuid::new_v4());
            let payload = json!({
                "goal_id": goal_id,
                "after_sequence": after,
                "wait_ms": EVENT_WAIT_MS,
            });
            match gateway.send("goal.events", payload, &key).await {
                Ok(reply) => {
                    if let Ok((events, last)) = parse_events(reply) {
                        if !events.is_empty()
                            && tx
                                .send(RunnerMsg::Ui(TuiAction::EventsReceived { events }))
                                .is_err()
                        {
                            return;
                        }
                        after = after.max(last);
                    }
                }
                Err(GatewayError::Transport(reason)) => {
                    let _ = tx.send(RunnerMsg::Ui(TuiAction::Disconnected { reason }));
                    return;
                }
                Err(_) => {}
            }
        }
    });
}

/// Probe until the supervisor answers, then announce Connected.
fn start_reconnect<G: TuiGateway>(gateway: G, tx: UnboundedSender<RunnerMsg>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(RECONNECT_INTERVAL).await;
            if gateway.ping().await {
                let _ = tx.send(RunnerMsg::Ui(TuiAction::Connected));
                return;
            }
            if tx.is_closed() {
                return;
            }
        }
    });
}

// ── Terminal input mapping ─────────────────────────────────────────────

fn terminal_event_loop(tx: UnboundedSender<RunnerMsg>) {
    loop {
        let action = match crossterm::event::read() {
            Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => map_key(key),
            Ok(Event::Resize(cols, rows)) => Some(TuiAction::Resize { cols, rows }),
            Ok(_) => None,
            Err(_) => return,
        };
        let Some(a) = action else { continue };
        if tx.send(RunnerMsg::Ui(a)).is_err() {
            return;
        }
    }
}

/// Map a crossterm key into a transport-agnostic reducer action.
pub fn map_key(key: KeyEvent) -> Option<TuiAction> {
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return match key.code {
            // Ctrl+C exits the TUI; it NEVER cancels the goal.
            KeyCode::Char('c') => Some(TuiAction::Quit),
            KeyCode::Char('u') => Some(TuiAction::Key(KeyIntent::ClearLine)),
            _ => None,
        };
    }
    let k = |intent: KeyIntent| TuiAction::Key(intent);
    match key.code {
        KeyCode::Enter => Some(TuiAction::Submit),
        KeyCode::Char(c) => Some(TuiAction::CharInput(c)),
        KeyCode::Backspace => Some(k(KeyIntent::Backspace)),
        KeyCode::Delete => Some(k(KeyIntent::Delete)),
        KeyCode::Left => Some(k(KeyIntent::Left)),
        KeyCode::Right => Some(k(KeyIntent::Right)),
        KeyCode::Home => Some(k(KeyIntent::Home)),
        KeyCode::End => Some(k(KeyIntent::End)),
        KeyCode::Up => Some(k(KeyIntent::Up)),
        KeyCode::Down => Some(k(KeyIntent::Down)),
        KeyCode::PageUp => Some(k(KeyIntent::PageUp)),
        KeyCode::PageDown => Some(k(KeyIntent::PageDown)),
        KeyCode::Tab => Some(k(KeyIntent::Tab)),
        KeyCode::Esc => Some(k(KeyIntent::Esc)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    #[test]
    fn ctrl_c_maps_to_quit_not_cancel() {
        let mut k = key(KeyCode::Char('c'));
        k.modifiers = KeyModifiers::CONTROL;
        assert!(matches!(map_key(k), Some(TuiAction::Quit)));
    }

    #[test]
    fn enter_submits_and_chars_type() {
        assert!(matches!(
            map_key(key(KeyCode::Enter)),
            Some(TuiAction::Submit)
        ));
        assert!(matches!(
            map_key(key(KeyCode::Char('中'))),
            Some(TuiAction::CharInput('中'))
        ));
    }

    #[test]
    fn navigation_keys_map_to_intents() {
        assert!(matches!(
            map_key(key(KeyCode::PageUp)),
            Some(TuiAction::Key(KeyIntent::PageUp))
        ));
        assert!(matches!(
            map_key(key(KeyCode::Tab)),
            Some(TuiAction::Key(KeyIntent::Tab))
        ));
    }
}
