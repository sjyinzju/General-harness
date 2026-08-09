# I8B — Ratatui TUI Shell + Interactive Goal Console: Implementation Plan

Status: investigation-complete implementation plan (written before code, per §71).
Baseline START_HEAD: `2c09cab9e32d74fd25234a5be4ec0001ba062aa8` (I8A sealed, main, clean tree).

This document answers every §71 investigation question from real source, then
fixes the module layout, the additive presentation changes, and the test plan.

---

## 1. Source investigation answers

| Question | Answer (verified in source) |
|---|---|
| CLI entry point | `crates/harness-cli/src/main.rs`. No-arg invocation currently calls `print_usage()` and exits — it has formal semantics, so the TUI entry must be TTY-gated (see §3). |
| Supervisor bootstrap | `crates/harness-cli/src/commands/supervisor.rs::cmd_supervisor_start` spawns a detached `harness-cli supervisor run` child (Windows: `CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS`). Reachability check: `ipc_client::SupervisorClient::ping()` (`health` command). The TUI reuses both — no second daemon. |
| IPC client | `crates/harness-cli/src/ipc_client.rs::SupervisorClient` — one Named Pipe connection per request/response, length-prefix framing from `harness_runtime::ipc::{framing,transport}`. Envelope: `IpcRequestEnvelope{request_id, idempotency_key, command, payload}` (`harness-core/src/contracts/ipc.rs`). |
| SubmitGoal | `goal.create` (payload = full `GoalSpec` JSON; `cmd_goal_create` in `supervisor/command_handler.rs`) then `goal.start` (Draft → Planning + `start_loop_run`). The client supplies `goal_id` in the spec (see `make_test_goal` in `tests/ipc_interaction_tests.rs`), which makes create retry-safe against the `goals.goal_id` PK. |
| GoalSnapshot DTO fields | `harness-core/src/contracts/presentation.rs`: `goal{id,revision,title,objective,state,budget,approval_policy,timestamps}`, `active_plan`/`latest_plan{id,revision_number,state}`, `tasks[]{planned_task_id,client_ref,title,state,dependencies,risk,requires_approval,expected_evidence,materialized ids}`, `pending_interactions[]{approval_id,kind,plan_revision_id,reason,requested_action,created_at}`, `interventions[]`, `running_activities[]{run_id,state,iteration_number,plan_revision_id}`, `usage: UsageSummary`, `last_event_sequence`. |
| PresentationEvent variants | `PresentationEvent{sequence, goal_id, event_type, occurred_at, payload}` over `goal_events`. Observed types: `goal_state_changed`, `plan_activated`, `clarification_requested`, `clarification_answered`, `plan_approval_requested`, `plan_approved`, `plan_revision_requested`, `user_intervention_received`, `goal_paused`, `goal_resumed`, `goal_cancelled`, `goal_succeeded`, task/verification pipeline events. The reducer treats `event_type` as data, never fabricating variants. |
| UsageSummary fields | `UsageSummary{totals{input_tokens?,output_tokens?,cached_input_tokens?,tool_calls?,wall_time_ms?,estimated_cost_micros?}, usage_known, sources[], per_profile[]}` — projected server-side from `task_usage_ledger`; unknown stays `None`. |
| goal.events contract | `cmd_goal_events`: `{goal_id, after_sequence, wait_ms?}` → `{events[], count, last_sequence}`; `wait_ms` capped at 30 000; 100 ms server poll; up to 100 events per batch. |
| Interactive approval policy | `GoalSpec.approval_policy.require_initial_plan_approval = true` (existing field, `harness-core/src/contracts/goal.rs::ApprovalPolicy`). No IPC wiring change needed: the TUI submits a full GoalSpec with this flag set. NonInteractive default (flag false) untouched. |
| Existing goal list | `goal.list` exists (`cmd_goal_list` → `{goals:[{goal_id,title,revision}], count}`, state-filter supported, read-only, non-ledgered). It lacks `state`/timestamps → additive read-only projection extension (§4.3). |
| TUI module/crate location | `crates/harness-cli/src/tui/`. Rationale: `ratatui = "0.28"` and `crossterm = "0.28"` are **already declared in harness-cli's Cargo.toml** (currently unused); dependency-rules.md §5 requires an ADR before a 5th crate; harness-cli already depends on harness-core contracts + the IPC transport. No new crate, no new dependency. |

## 2. Hard boundary compliance

```
TUI (harness-cli/src/tui)
  → depends only on: harness_core::contracts::{ipc,presentation,goal}
                     + harness_runtime::ipc::{framing,transport} (pipe I/O only)
  → SupervisorClient-style IPC requests
  → Supervisor (Request Ledger) → GoalLoopService → Repository
```

- The TUI module imports **no** repository, service, sqlx, or migration code.
- The TUI never opens the business DB; every mutation is an IPC request.
- User input is data: text goes to `goal.intervene` / `goal.answer` /
  `goal.request_changes` payloads; never to `Command::new`.

## 3. Entry behavior (compatibility strategy)

`main()` today: `len < 2 → print_usage()`. I8B changes this single branch:

- `len < 2` **and stdout is a TTY** → enter the TUI.
- `len < 2` **and not a TTY** (CI, pipes, scripts) → keep `print_usage()` byte-for-byte.
- Every existing subcommand/flag path is untouched (`--standalone`, `goal *`, …).

Supervisor bootstrap on TUI entry:
1. `SupervisorClient::ping()` → if reachable, connect.
2. If unreachable: reuse `cmd_supervisor_start` mechanics (spawn detached
   `supervisor run` child with the resolved `--repo/--db`), then poll `health`
   with bounded backoff (≤ ~10 s) before entering the shell; on failure show a
   readable error panel (never a raw Rust debug dump).

## 4. Additive presentation changes (Gap Policy §6 — DTO only, zero migrations)

Identified gaps and their minimal fixes:

### 4.1 `SnapshotTask` agent/model display (§26, §34, §67)
`planned_tasks` has no agent columns; the assignment is only known once a task
is materialized. Add to `SnapshotTask`:

```rust
#[serde(default)] pub agent_kind: Option<String>,  // runtime_profiles.agent_kind
#[serde(default)] pub model: Option<String>,       // runtime_profiles.model
#[serde(default)] pub provider: Option<String>,    // runtime_profiles.provider
```

Filled in `cmd_goal_snapshot` (implemented join path, verified against the
schema): `planned_tasks.materialized_task_id → tasks.current_execution_id →
execution_attempts.profile_id → runtime_profiles{agent_kind, model, provider}`,
with `NULLIF(col, '')` so empty strings stay `None`. Non-materialized tasks →
`None` → UI prints `unknown` (never fabricated). All new DTO fields are
`#[serde(default)]` — additive, zero migrations.

### 4.2 `RunningActivity` task-level detail (§35)
Add optional fields so the Activity panel can show the executor, not just the
loop run:

```rust
#[serde(default)] pub task_title: Option<String>,
#[serde(default)] pub agent_kind: Option<String>,
#[serde(default)] pub model: Option<String>,
```

Filled from the `running` planned task of the active plan via
`planned_tasks → tasks → execution_attempts → runtime_profiles`; absent values
render as `unknown`.

### 4.3 `goal.list` projection (§20)
`cmd_goal_list` gains `state`, `created_at`, `updated_at` per item via a
single light read of the `goals` table (read-only; no ledger; no schema
change). DTO shape: `{goal_id, title, revision, state, created_at, updated_at}`.

### 4.4 What is NOT needed
- No new business table, no migration, no Interaction FSM change.
- `UsageSummary`, `PendingInteraction`, snapshot cursor: sufficient as-is.
- No `I8A_PRESENTATION_CONTRACT_GAP` blocker found.

## 5. Module layout (`crates/harness-cli/src/tui/`)

```
tui/
  mod.rs        — pub run_tui(TuiOptions) entry; module wiring
  state.rs      — TuiAppState projection state (never durable)
  action.rs     — TuiAction (terminal / ipc / tick) + Effect (outbound intents)
  reducer.rs    — pure reduce(state, action) -> Vec<Effect>; cursor rules
  input.rs      — InputBuffer single-line editor (unicode-safe, no panics)
  commands.rs   — /help /plan /status /usage /pause /resume /cancel /quit
                  /clear /goals /goal <id>
  spec.rs       — build_interactive_goal_spec() (client-side GoalSpec JSON)
  gateway.rs    — trait TuiGateway + PipeGateway (Named Pipe, retry-stable keys)
  runner.rs     — tokio event loop: terminal task + long-poll task + tick
  terminal.rs   — TerminalGuard RAII (raw mode, alt screen, panic hook)
  widgets/
    mod.rs      — layout composition
    header.rs   — title/project/goal/state/connection/elapsed/usage
    panels.rs   — PLAN/TASKS + ACTIVITY
    conversation.rs
    input.rs    — contextual prompt + buffer line
    modal.rs    — clarification / approval / cancel-confirm / help / error
```

### 5.1 `TuiAppState` (projection only)
`connection`, `active_goal_id`, `snapshot: Option<GoalSnapshot>`,
`cursor: i64`, `conversation: Vec<ConversationEntry>`, `pending: PendingUi`
(clarification modal state incl. per-question answers, approval mode,
cancel-confirm), `input: InputBuffer + InputMode`, `focus/scroll offsets`,
`usage`, `toast/error`, `goals_list`, `exit_requested`, `resync_needed`.

### 5.2 Reducer rules
- `apply_snapshot`: replaces projection wholesale, cursor = snapshot cursor,
  rebuilds conversation from snapshot (pending interactions, interventions,
  goal header) — reconnect never depends on in-memory history.
- Event fold: `seq <= cursor` → ignore (duplicate); `seq == cursor+1` → apply
  + advance; `seq > cursor+1` → set `resync_needed` (Effect::Resnapshot), no
  blind application.
- `/quit`, Ctrl+C → `exit_requested` only — **never** `goal.cancel`.
- `/cancel` → confirmation modal first; only on `y` emit Effect::GoalCancel.
- Mutations emit Effects carrying a **stable idempotency key** generated once
  per user action (stored in state until success/conflict) — retry after
  timeout reuses key+payload; `Duplicate` response = success; `Conflict` →
  user-visible error toast, never silent overwrite.

### 5.3 Event loop (no busy loop)
- Terminal events: dedicated blocking task (`crossterm::event::poll` w/ 200 ms
  timeout) → mpsc.
- Events: background task long-polls `goal.events(after=cursor, wait_ms=10000)`;
  never blocks the UI task.
- Tick: 250 ms interval only redraws when dirty/elapsed needs refresh.
- Disconnect: header `Connection: reconnecting…`, backoff 250 ms → 5 s cap;
  on success re-fetch snapshot (snapshot is the resync authority).

### 5.4 Input routing (§23)
Mode derived from state: no goal → `Goal >` (SubmitGoal); pending
clarification → `Answer >`; approval + RequestChanges mode → `Plan changes >`;
goal active → `Message >` (intervene); goal terminal → `New goal >`.
Multiple clarification questions: collect answers per question in the modal,
submit one `goal.answer` with the full answers payload.

### 5.5 Terminal lifecycle
`TerminalGuard`: enter raw mode + alternate screen + hide cursor; `Drop` and a
registered panic hook restore (raw off, leave alt screen, show cursor) on
every path: normal exit, error, Ctrl+C, panic. Resize handled via the
`Event::Resize` redraw; below-minimum sizes render a "Terminal too small"
notice, never panic.

## 6. Test plan (0 real LLM calls)

| Suite | Location | Coverage |
|---|---|---|
| Render tests | `tui/render.rs` unit tests (ratatui `TestBackend`) | empty state, planning, clarification modal, plan approval, running tasks, paused, failed task, completed goal, disconnected, terminal-too-small, usage unknown, usage populated (12) |
| Reducer tests | `tui/reducer.rs` unit tests | snapshot init, duplicate ignored, next-seq applied, gap → resync, snapshot replaces stale, goal/task state updates, clarification pending/answered, approval request/completed, plan revision, intervention, pause, resume, completion (16+) |
| Input routing tests | `tui/reducer.rs` + `input.rs` | no-goal Enter → SubmitGoal; clarification Enter → Answer; request-changes Enter → RequestChanges; running Enter → Intervene; /pause /resume /cancel(confirm-first) /quit; Chinese input no panic |
| Ledger retry tests | `tui/gateway.rs` + reducer | timeout retry reuses same key+payload; Duplicate → success; Conflict → UI error |
| Terminal guard tests | `tui/terminal.rs` | cleanup executed on normal + error paths (lifecycle abstraction) |
| Integration | `crates/harness-cli/src/tui/integration_tests.rs` (cfg(test) — the one designed exception to the boundary) | Real `IpcServer` over a unique Named Pipe endpoint per test, real `SupervisorCommandHandler` + `ProductionGraph` + in-memory SQLite: submit goal → snapshot → clarification answer (ledger Duplicate replay) → plan approve/request-changes → intervene → events long-poll → pause/resume/cancel with Conflict handling → gapless reconnect (snapshot cursor + events) |
| Regression | existing | I8A 42 tests (`interaction_protocol.rs` 25 + `ipc_interaction_tests.rs` 17) + full workspace |

Quality gates: `cargo fmt --all --check`, `cargo clippy --workspace
--all-targets -- -D warnings`, `cargo test --workspace --no-fail-fast`,
`cargo build --workspace`.
Disk: `CARGO_TARGET_DIR=E:\General-harness\target\scratch`,
`CARGO_INCREMENTAL=0`, test temp under harness-controlled scratch.

## 7. Commit strategy

- **A** `feat(tui): add Ratatui application shell and projection state` —
  TUI modules (state/action/reducer/input/render/widgets/terminal), render +
  reducer + routing tests, TTY-gated entry stub.
- **B** `feat(tui): connect interactive goal control over IPC` — gateway,
  runner, goal submit/clarification/approval/request-changes/intervene/
  pause/resume/cancel, reconnect, additive presentation DTO fields (§4),
  goal.list extension, integration tests.
- **C** `docs(i8b): finalize TUI architecture and certification` — this plan +
  `verification/I8B_FINAL_REPORT.md`.

## 8. Explicit non-goals (per brief §3)

No I8C first-run setup, no agent/provider/model wizards, no secret stores, no
token-accounting adapters, no installer/release/auto-update, no GUI framework,
no second daemon, no business-DB access from the TUI, no I1–I8A refactor.
