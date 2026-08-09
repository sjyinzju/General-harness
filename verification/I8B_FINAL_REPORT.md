# I8B — Ratatui TUI Shell + Interactive Goal Console: Final Certification Report

Status: **PASS**
Date: 2026-08-09
Branch: main (no worktree; brief mandated direct commits on main, no push)

## 1. Baseline and commits

| Item | Value |
|---|---|
| START_HEAD | `2c09cab9e32d74fd25234a5be4ec0001ba062aa8` (I8A sealed, clean tree) |
| Commit A | `0bb8810 feat(tui): add Ratatui application shell and projection state` |
| Commit B | `2636e8b feat(tui): connect interactive goal control over IPC` |
| Commit C | this report + `docs/I8B_TUI_IMPLEMENTATION_PLAN.md` (`docs(i8b): finalize TUI architecture and certification`) |
| FINAL_HEAD | `d59dcda` (Commit C; amended in place with this SHA record) |

## 2. Architecture delivered

**Entry** — `crates/harness-cli/src/main.rs`: no-arg + stdout-is-TTY → TUI;
no-arg + non-TTY → byte-identical `print_usage()`; `HARNESS_NO_TUI=1` escape
hatch. Every existing subcommand untouched.

**Boundary (first principle)** — the TUI (`crates/harness-cli/src/tui/`) is a
pure projection client. Production code in the module depends only on
`harness_core::contracts::{ipc,presentation,goal}` and
`harness_runtime::ipc::{framing,transport}` (pipe I/O). It imports **no**
repository, service, sqlx, or migration code and **never opens the business
DB**. Every mutation is a ledgered IPC request. User input is always data
(payload text to `goal.intervene` / `goal.answer` / `goal.request_changes`),
never a command.

**Data path** — TUI → Named Pipe IPC → Supervisor `SupervisorCommandHandler`
→ Request Ledger → `GoalLoopService` / repositories. Snapshot is the resync
authority; events carry interaction truth only; task detail refreshes ride
`panels_dirty` + re-snapshot.

**Cursor rules** — `seq <= cursor` ignored (duplicate); `seq == cursor+1`
applied and cursor advanced; `seq > cursor+1` sets `resync_needed` →
re-snapshot, never blind application. Reconnect = snapshot first, then events
from snapshot cursor → gapless.

**Idempotency** — every mutation carries one stable client-generated key per
user action; retry after timeout reuses key+payload; `Duplicate` = success;
`Conflict` = user-visible toast, never silent retry/overwrite.
`goal.create`/`goal.start` retry-safety comes from the client-supplied
`goal_id` PK. Quitting the TUI never cancels a goal; `/cancel` requires an
explicit `y` confirmation modal.

**Agent/model display (§67)** — additive `#[serde(default)]` DTO fields only
(`SnapshotTask.{agent_kind,model,provider}`,
`RunningActivity.{task_title,agent_kind,model}`). Snapshot builder resolves
the real runtime assignment:
`planned_tasks.materialized_task_id → tasks.current_execution_id →
execution_attempts.profile_id → runtime_profiles`, with `NULLIF` keeping empty
strings as `None`. No assignment → UI prints `unknown`. No hardcoded
Claude/Codex/model strings anywhere. Zero migrations, zero new tables, zero
Interaction FSM change.

**goal.list projection (§20)** — additive read-only extension:
`{goal_id,title,revision,state,created_at,updated_at}` via a direct projection
on `goals`.

## 3. Module layout

`crates/harness-cli/src/tui/`: `mod.rs` (boundary docs + wiring), `state.rs`,
`action.rs` (Action/Effect), `reducer.rs` (pure reduce → Vec<Effect>),
`input.rs` (unicode-safe single-line editor), `commands.rs` (slash commands),
`spec.rs` (interactive GoalSpec builder), `gateway.rs` (trait + PipeGateway),
`runner.rs` (tokio event loop: terminal task, long-poll task, 250 ms tick,
reconnect backoff 250 ms → 5 s), `terminal.rs` (TerminalGuard RAII: raw mode +
alt screen + panic hook restore on every exit path), `widgets.rs` (header /
PLAN+TASKS / ACTIVITY / conversation / contextual input / modals), `render.rs`
(layout + TestBackend tests), `integration_tests.rs` (cfg(test) real-pipe
suite — the one designed boundary exception).

## 4. UX flows implemented

- **Submit goal** — empty-state Enter submits an interactive GoalSpec
  (`require_initial_plan_approval = true`) via `goal.create` + `goal.start`.
- **Clarification** — modal per question (`Answer >`), required-answer guard,
  single `goal.answer` with answers map keyed by `question_id`.
- **Plan approval** — `a` approve (with `expected_plan_revision_id`), `e`
  request changes (`Plan changes >` feedback → `goal.request_changes`),
  reject path via intervention.
- **User intervention** — free text on an active goal → `goal.intervene`;
  surfaces in conversation + events.
- **Pause/Resume/Cancel** — `/pause` `/resume` `/cancel` (confirm modal),
  ledgered, Duplicate/Conflict handled per policy.
- **Panels** — header (goal/state/connection/elapsed/usage), PLAN/TASKS with
  real `[agent · model]` or `[agent: unknown]`, ACTIVITY with task title +
  executor, scrolling conversation, usage projection (unknown rendered as
  unknown), toast/error line, help modal, terminal-too-small notice.
- **Console commands** — `/help /plan /status /usage /goals /goal <id>
  /pause /resume /cancel /clear /quit`.

## 5. Tests (0 real LLM calls)

| Suite | Count | Result |
|---|---|---|
| Render tests (`render.rs`, TestBackend) | 14 | PASS |
| Reducer tests (`reducer.rs`) | 20 | PASS |
| Input editor (unicode/Chinese no-panic) | 6 | PASS |
| Console commands | 3 | PASS |
| Gateway (retry key reuse, Duplicate/Conflict) | 2 | PASS |
| Runner loop | 3 | PASS |
| GoalSpec builder | 2 | PASS |
| Terminal guard lifecycle | 2 | PASS |
| IPC-TUI integration (real Named Pipe + real services + real SQLite) | 7 | PASS |
| harness-cli bin total | 61 | PASS |
| I8A regression (`interaction_protocol` 25 + `ipc_interaction_tests` 17) | 42 | PASS, zero regressions |
| Full workspace `cargo test --workspace --no-fail-fast` | all suites (0 failed) | PASS |

Integration suite covers over a live pipe: goal submit round-trip → snapshot;
clarification answer with ledger Duplicate replay; plan approval via decision
key activates goal (Duplicate replay verified); request-changes returns goal
to planning; intervention + `goal.events` long-poll advances cursor; pause/
resume/cancel incl. same-key-different-command Conflict surfaced in UI with no
side effect; reconnect = snapshot-then-events gapless.

A real bug was found and fixed by these tests: the reducer originally sent
`answers` as an array; the I8A contract is a map keyed by `question_id`. Fixed
in `reducer.rs` before Commit B sealed.

## 6. Quality gates (all PASS)

| Gate | Command | Result |
|---|---|---|
| Format | `cargo fmt --all --check` | PASS (exit 0) |
| Clippy | `cargo clippy --workspace --all-targets -- -D warnings` | PASS (exit 0) |
| Tests | `cargo test --workspace --no-fail-fast` | PASS (0 failed) |
| Build | `cargo build --workspace` | PASS (exit 0) |

## 7. Disk hygiene

All builds/tests ran with `CARGO_TARGET_DIR=E:\General-harness\target\scratch`,
`CARGO_INCREMENTAL=0`, `TEMP/TMP=E:\General-harness\.scratch\tmp`. No
user-profile temp growth, no stray business DB created by the TUI.

## 8. Manual TTY smoke

**NOT AVAILABLE** — this execution environment is non-interactive (no real
TTY; the TUI's entry gate itself refuses to launch on a non-TTY stdout, which
is exactly the CI-safety behavior being certified). All interactive behavior
is instead proven by TestBackend render tests (14) and real-pipe integration
tests (7) exercising the identical reducer/gateway code paths a human session
would drive.

## 9. Findings / blockers

- Blocking findings: **0** (`I8B_PRESENTATION_CONTRACT_GAP`: none).
- Non-blocking, fixed in-stage: answers-payload shape mismatch (§5).
- Additive-only changes outside `tui/`: `presentation.rs` DTO fields,
  `command_handler.rs` snapshot builder + `goal.list` projection. No schema,
  no FSM, no ledger semantic changes.
- `verification/I8A_FINAL_REPORT.md` untouched (§77).

## 10. Verdict

`PASS — I8B Ratatui TUI Shell complete.`
`I8B STATUS: COMPLETE`
`NEXT: I8C — Do not start I8C automatically.`
