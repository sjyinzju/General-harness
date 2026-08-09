# I8A — Human Interaction Protocol + TUI Architecture Foundation

Status: I8A implementation reference.
Baseline START_HEAD: `c5c7f1cdd5c07bb7f2211d97633006fe9f9da316` (I1–I7 sealed).

This document has two parts:

1. **Existing Capabilities Audit** — what I1–I7 already provides, classified
   REUSE / EXTEND / NEW.
2. **Interaction Architecture** — the durable Human Interaction domain,
   protocols, IPC surface, snapshot/event model, and the I8B TUI boundary.

---

## Part 1 — Existing Capabilities Audit

### 1.1 Production chain (verified, sealed)

```
CLI → Named Pipe IPC → Supervisor → OperationIntent/Request Ledger
    → GoalLoopService → Planner → PlanRevision/PlannedTask
    → Scheduler/I4.5 dispatch → Verification → Candidate → Review
    → Controlled Commit → Integration → GoalObservation → Evaluator
    → CompletionPolicy → Succeeded/Replan
```

### 1.2 Audit table

| Capability | Where it lives today | I8A classification |
|---|---|---|
| IPC envelope (`request_id`, `idempotency_key`, command whitelist, framing) | `harness-core/src/contracts/ipc.rs`, `harness-runtime/src/ipc/` | **REUSE** (envelope already carries `request_id`/`idempotency_key`) / **EXTEND** (the server drops them before `handle_command`; I8A threads a request context through) |
| Named Pipe transport, length-prefix framing | `ipc/transport.rs`, `ipc/framing.rs` | **REUSE** unchanged |
| Request Ledger — `idempotency_records` claim API (`try_claim`/`complete_claim`/`get_result`, version-CAS, lease takeover) | `harness-runtime/src/idempotency.rs` | **REUSE** as the ledger for all interaction mutations |
| `operation_intents` (kind CHECK frozen in migration 027 to task/review/integration kinds) | `migrations/027`, `supervisor/command_handler.rs::persist_operation_intent` | **REUSE for existing kinds only.** NOT extended: the CHECK constraint is sealed; interaction mutations ledger through `idempotency_records` instead (same exactly-once discipline, no schema rewrite) |
| Goal FSM with `waiting_for_approval`, `paused`, `blocked` states | `harness-core/src/state_machine/goal_fsm.rs`, `goals.state` CHECK | **REUSE** — no new Goal states needed. `WaitingForApproval` is the waiting-for-user semantic (clarification + plan approval); `Paused` is the pause semantic. Both already have legal transitions in/out |
| `approval_requests` table + `ApprovalRequest`/`ApprovalType` (incl. `approve_initial_plan`, `provide_missing_information`) with `plan_revision_id` binding and `payload_digest` | `migrations/028`, `goal/mod.rs`, `goal/repo.rs` | **REUSE the aggregate / EXTEND the schema** — additive migration 030 adds `response_json`, `request_id`, `source` columns. Clarification and plan approval are both `approval_requests` rows; no parallel "interaction_requests" table is created |
| `GoalSpec.approval_policy` (`require_initial_plan_approval`, …, `approval_timeout_secs`) | `harness-core/src/contracts/goal.rs` | **REUSE** as the Interactive/NonInteractive switch. NonInteractive = all flags false (current I1–I7 behavior, unchanged default) |
| Goal pause/resume/cancel IPC commands (`goal.pause/resume/cancel` → `transition_goal`) | `supervisor/command_handler.rs` | **EXTEND** — transitions exist but (a) are not idempotent on repeat, (b) are not ledgered, (c) `drive_goal_loop` never checks `Paused` before dispatch. I8A adds the dispatch gate + idempotent replay + ledger |
| Cancellation | `goal.cancel` → FSM `Cancelled`; task-level `task.cancel` + production cancellation | **REUSE** — `/cancel` in the future TUI is the existing `goal.cancel` IPC request. No second cancellation system |
| Per-goal ordered event stream (`goal_events.sequence_num`) + `goal.events` with `after_sequence` resume | `migrations/028`, `goal/repo.rs::append_goal_event`, `cmd_goal_events` | **REUSE/EXTEND** — the resumable sequence already exists. I8A adds: atomic sequence allocation (single-statement INSERT…SELECT + UNIQUE index), interaction event types, and a `wait_ms` long-poll so it behaves as `SubscribeGoalEvents(goal_id, after_sequence)` |
| `event_log` (stream_id/stream_version) | `event_log.rs` | **REUSE** for aggregate streams; presentation events project from `goal_events` (per-goal total order) — no domain-event rewrite |
| Planner structured output (`PlanProposal` parse + validation) | `goal/planner.rs`, `goal/mod.rs` | **EXTEND** — planner outcome becomes `Plan(PlanProposal) | ClarificationNeeded(questions)`; prompt/schema additively extended |
| Planner context assembly (`build_planning_context`) | `goal/service.rs` | **EXTEND** — inject clarification answers, user interventions, and requested plan changes into `GoalPlanningContext` |
| Plan activation (validated → active, supersede old) | `goal/service.rs::activate_plan` | **EXTEND** — interactive mode stops at `Validated` + creates a plan-approval request; activation happens on Approve |
| Scheduler dispatch saga (idempotent dispatch intents, reservations, leases, fencing) | `scheduler/dispatch.rs`, `task_loop` | **REUSE** unchanged — pause gates live in the Goal loop *before* materialization, not inside the sealed dispatch saga |
| Usage ledger (`task_usage_ledger` with `usage_known`, `usage_source` ∈ provider_reported/estimated/unknown) | `migrations/021` | **REUSE** — `UsageSummary` DTO is a pure projection; unknown stays unknown, never estimated as actual |
| Crash recovery (supervisor reconciliation, `continue_incomplete_pipelines_for_plan`, failpoints) | `goal/failpoint.rs`, recovery runs | **REUSE** — interaction state is plain durable rows + FSM states, so existing restart semantics recover it; new failpoint-style tests cover the interaction windows |
| CLI goal subcommands (incl. `goal answer/approve/reject/events`) | `harness-cli/src/main.rs` | **REUSE/EXTEND** — `goal.answer` today is a stub server-side; CLI gains `snapshot`, `intervene`, `request-changes` passthroughs |
| Event push subscription over IPC | `Subscribe`/`Unsubscribe` return UnsupportedCommand | **NEW (bounded)** — implemented as long-poll resume on `goal.events`, not a new push protocol (framing stays request/response) |
| User intervention / user message concept | — (nothing exists) | **NEW** — `user_interventions` table + service |
| Goal snapshot API | — (only `goal.show`/`status` fragments) | **NEW** — `goal.snapshot` read-only projection |
| Presentation DTOs (snapshot, events, usage) | — | **NEW** — `harness-core/src/contracts/presentation.rs` |

### 1.3 Key findings that shaped the design

1. **The Goal FSM already models waiting-for-user.** `WaitingForApproval` has
   legal transitions `Planning→WFA`, `Active→WFA`, `WFA→Active`, `WFA→Planning`.
   Clarification and plan approval both park the goal here. No state enum
   changes; maximum crash-recovery reuse.
2. **`approval_requests` is the right aggregate.** It already binds
   `plan_revision_id`, carries `payload_digest`, has a pending→resolved life
   cycle, and has types for both plan approval and missing information. What it
   lacks (answer payload, originating `request_id`) is purely additive.
3. **The pause state exists but is not enforced.** `goal.pause` flips
   `goals.state`, but `drive_goal_loop` never reads it before dispatching — a
   paused goal keeps dispatching. The I8A dispatch gate closes this.
4. **`operation_intents.operation_kind` CHECK is sealed** (migration 027). New
   interaction kinds cannot be inserted without rebuilding the table. The
   generic `idempotency_records` claim ledger provides the same exactly-once
   guarantee and is already the I2 "Request Ledger", so interaction mutations
   use it. This is a deliberate reuse, not a second ledger: one durable claim
   per client `idempotency_key`, replay returns the stored result.
5. **The IPC server drops envelope identity.** `handle_command(&command,
   &payload)` never sees `request_id`/`idempotency_key`, so no goal mutation is
   ledgered today. I8A introduces `IpcRequestContext` threaded via a new
   default-implemented trait method (backward compatible with existing
   handlers/tests).
6. **`append_goal_event` has a read-then-insert race** (MAX+1 in two
   statements). With IPC handlers and the goal loop appending concurrently,
   I8A makes it a single atomic `INSERT … SELECT COALESCE(MAX…)+1` and adds a
   UNIQUE `(goal_id, sequence_num)` index.
7. **The background goal loop self-terminates on stall** (no-progress > 10
   iterations). Waiting minutes for a human answer would kill it. Interaction
   resolutions therefore call `ensure_loop_run(goal_id)` to (re)start the loop
   — safe because at most one active run per goal is enforced by a partial
   unique index.

---

## Part 2 — Interaction Domain Design

### 2.1 First principle: the TUI is never a second business writer

```
TUI / CLI
  → IPC (Named Pipe, request_id + idempotency_key)
    → Supervisor (Request Ledger claim)
      → InteractionService / GoalLoopService (production writers)
        → Repository (SQLite, single writer, FSM-validated)
```

The TUI never touches SQLite business tables, never resolves approvals
directly, never mutates `planned_tasks`, never calls the Planner. All state it
displays comes from `goal.snapshot` + `goal.events`; all input it sends is an
IPC mutation carrying `request_id`/`idempotency_key`.

User natural-language input is **data**, never a command: interventions are
stored and routed into planner context; nothing in the interaction path ever
reaches a shell.

### 2.2 Aggregates

Two durable aggregates cover the whole protocol (deliberately not ten tables):

**A. InteractionRequest = existing `approval_requests` (extended).**
A harness→user request that blocks or gates progress, in two kinds:

- `provide_missing_information` — **ClarificationRequest**. `requested_action_json`
  holds `{questions: [{question_id, prompt, choices?, required, reason}]}`.
- `approve_initial_plan` — **PlanApprovalRequest**. Bound to a concrete
  `plan_revision_id`; `requested_action_json` holds the plan summary shown to
  the user (goal summary, revision number, tasks with dependencies / role /
  agent / profile / verification strategy).

New additive columns (migration 030): `response_json` (the user's answer or
decision detail), `request_id` (originating IPC request), `source`.
Resolution reuses the existing 5-state CHECK:
`pending → approved` (answered / approved), `→ rejected` (rejected or
request-changes; decision detail in `response_json`), `→ cancelled`
(superseded by a newer revision), `→ expired`.

**B. UserIntervention = new `user_interventions` table.**
A user→harness message that does *not* block progress by itself:

```
intervention_id, goal_id, request_id, source, message,
classification (informational | constraint_addition | plan_change_required
               | pause_requested | cancel_requested),
state (received | applied | superseded),
created_at, processed_at, applied_plan_revision_id
```

Rationale: requests-from-harness and messages-from-user have different life
cycles (one is resolved by exactly one reply; the other accumulates and is
consumed by future planning), so they are separate aggregates — but each side
is a single table.

Pause/Resume/Cancel are **not** new aggregates: they are FSM transitions on
`goals` (durable by definition) recorded in `goal_events` and ledgered per
request.

### 2.3 Clarification protocol

```
Planner output = ClarificationNeeded{questions}
  → InteractionService.request_clarification (tx):
      insert approval_request(provide_missing_information, questions)
      goal: Planning → WaitingForApproval
      goal_event: clarification_requested
  → loop iteration returns (no dispatch, no planning retry)

User answers (goal.answer over IPC):
  → ledger claim(idempotency_key)
      resolve approval → approved, response_json = answers
      goal: WaitingForApproval → Planning
      goal_event: clarification_answered
      ensure_loop_run(goal_id)
    complete claim(result)
  → next planner invocation context includes original goal + questions + answers
```

- Crash after request commit: goal is durably `WaitingForApproval` with a
  pending approval row → snapshot shows the open question after restart.
- Crash after answer commit before replan: approval is `approved`, goal is
  `Planning` with no active plan → recovery path re-enters planning; no
  duplicate side effect (planner invocation idempotency unchanged).
- Duplicate answer (same `request_id`/key): ledger replays stored result;
  the approval row is resolved exactly once. A second, different answer to an
  already-resolved approval is rejected with `Conflict`.

### 2.4 Plan approval protocol

Interactive mode = `goal.approval_policy.require_initial_plan_approval == true`
(the existing field; applies to first plan and every replan revision).

```
Planner → PlanProposal → validate → PlanRevision persisted as VALIDATED (not active)
  → approval_request(approve_initial_plan, plan_revision_id = PR-N)
  → goal → WaitingForApproval; goal_event: plan_approval_requested
  → NO task dispatch (gate: dispatch requires an ACTIVE plan + Active goal)

goal.approve {approval_id, expected_plan_revision_id}:
  → ledger claim
      STALE GUARD: approval.plan_revision_id must == expected_plan_revision_id,
        approval must be pending, AND the bound revision must still be the
        latest revision for the goal (no newer PlanRevision row exists).
        Otherwise → Conflict("stale approval"), nothing mutated.
      resolve approval → approved
      activate_plan_revision: Validated → Active (supersede old ACTIVE plans)
      goal: WaitingForApproval → Active; goal_event: plan_approved
      ensure_loop_run
    complete claim

goal.request_changes {approval_id, expected_plan_revision_id, notes}:
  → ledger claim
      same stale guard
      resolve approval → rejected, response_json = {decision:"request_changes", notes}
      plan revision: Validated → Rejected
      goal: WaitingForApproval → Planning; goal_event: plan_revision_requested
      notes become replan context for the Planner → new PlanRevision N+1
      → new approval request; the old one is terminally resolved (auto-obsolete)
    complete claim

goal.reject {approval_id, expected_plan_revision_id, reason}:
  → as request_changes but goal → Cancelled (user abandons) — explicit reason kept.
```

Stale-approval safety under concurrency: activation and the latest-revision
check run inside one SQLite transaction; the partial unique index
`idx_plan_one_active_per_goal` is the final arbiter — two racing approvals of
different revisions cannot both activate.

NonInteractive mode (`require_initial_plan_approval == false`, the default):
`activate_plan` behaves exactly as I7 — validated then activated immediately,
autonomous behavior preserved, all existing tests unaffected.

### 2.5 Runtime user intervention

`goal.intervene {goal_id, message}` at any time while the goal is non-terminal:

1. Ledger claim → insert `user_interventions` row (`received`) + goal_event
   `user_intervention_received` → complete claim. Never blocks or double-writes
   against the goal loop: the insert is a single-row append; the loop only
   *reads* interventions at iteration boundaries.
2. **Classification is production-side**, not TUI-side. I8A ships a
   deterministic classifier in the InteractionService: interventions are
   classified `constraint_addition` by default (they become durable planner
   context on the next planning cycle) — the TUI sends text only. Explicit
   pause/cancel remain their own first-class IPC requests (`goal.pause`,
   `goal.cancel`); the classifier never shells out and never calls a
   side-channel LLM. Model-assisted classification (e.g. deciding
   `plan_change_required` from free text) is deferred to I8D and must go
   through the formal Planner orchestration when it lands.
3. Consumption: `build_planning_context` appends all `received` interventions
   as `USER DIRECTIVES` (marked as authoritative user input, above untrusted
   repo content); after a plan revision is created from them they are marked
   `applied` with `applied_plan_revision_id`.

Two different messages never overwrite each other (append-only rows, distinct
ids). Ordering vs executor result commits is durable: both are rows committed
through the same single-writer SQLite database; the planner reads a snapshot at
planning time.

### 2.6 Pause / Resume / Cancel and safe points

- `goal.pause` → FSM `Active|Blocked → Paused` (durable). **Idempotent**: if
  already `Paused`, replay/second request returns success without a transition.
- **Dispatch gate** (the actual enforcement, new in I8A): `drive_goal_loop`
  refuses to plan or dispatch when goal state ∈ {Paused, WaitingForApproval,
  terminal}; additionally `materialize_and_dispatch` re-checks the goal state
  immediately before each task materialization, so a pause committed mid-
  iteration stops subsequent dispatches in that same iteration.
- Safe-point semantics:
  - **ControlledCommit / Integration in flight**: never interrupted — their
    transactions are atomic (I5 invariants untouched). Pause only prevents new
    dispatch; in-flight pipeline stages run to their durable boundary.
  - **New task dispatch**: gated (see above) — this is the pause point.
  - **Running Executors**: allowed to finish by default; their results land as
    observations. Explicit termination remains the existing task-level
    production cancellation (`task.cancel`), not a new mechanism. If a later
    plan revision supersedes their task, the planned task is marked
    `superseded` (existing semantics) and the result is not dispatched further.
- `goal.resume` → `Paused → Active`, idempotent (already Active → success
  no-op), ensure_loop_run restarts the loop after supervisor restart.
- `goal.cancel` → existing FSM cancellation, unchanged; TUI `/cancel` is just
  this request. Idempotent replay on already-cancelled returns success.
- Crash after pause: `goals.state = 'paused'` is durable; the recovery scan
  does not resurrect dispatch because the gate reads the same state.

### 2.7 Event stream + snapshot (TUI reconnect model)

**Snapshot**: new read-only `goal.snapshot {goal_id}` returns a
`GoalSnapshot` DTO projected in one read pass:

```
goal (id, title, objective, state, budget, approval_policy, timestamps)
active/latest plan revision (id, revision_number, state)
tasks [{planned_task_id, title, state, dependencies, risk, requires_approval,
        assigned_role, agent_kind, runtime_profile/provider/model (from the
        pinned profile where materialized; planned assignment otherwise),
        verification strategy (expected_evidence)}]
pending_interactions [{approval_id, kind, plan_revision_id, questions/plan summary}]
interventions (recent, with classification/state)
running_activities (active loop run state, materialized running tasks)
usage: UsageSummary (see 2.8)
last_event_sequence  ← the resume cursor
```

**Events**: `goal.events {goal_id, after_sequence, wait_ms?}` — the existing
resumable query, extended with optional long-poll (server waits up to
`wait_ms` for the next event before returning empty). This gives
`SubscribeGoalEvents(goal_id, after_sequence)` semantics over the existing
request/response framing without inventing a push protocol; `Subscribe`/
`Unsubscribe` stay reserved.

Reconnect contract: `snapshot` → render → `events(after = last_event_sequence)`
→ apply deltas. Sequence numbers are per-goal, gapless-monotonic, enforced by
UNIQUE index; events are never mutated.

**PresentationEvent** is a projection DTO (`sequence`, `goal_id`,
`event_type`, `occurred_at`, `payload`) over `goal_events`. I8A guarantees the
interaction event types (`goal_state_changed`, `plan_activated`,
`clarification_requested/answered`, `plan_approval_requested`,
`plan_approved`, `plan_revision_requested`, `user_intervention_received`,
`goal_paused`, `goal_resumed`, `goal_succeeded`, …). Task/verification/review/
commit progress events continue to accrue in later milestones by appending to
the same per-goal stream — no architecture change.

### 2.8 UsageSummary (boundary only)

Pure projection from `task_usage_ledger` joined via materialized tasks of the
goal's plan revisions:

```
UsageSummary {
  totals: { input_tokens?, output_tokens?, cached_input_tokens?,
            tool_calls?, wall_time_ms?, estimated_cost_micros? },
  usage_known: bool,          // AND over rows; absent rows → false
  sources: [provider_reported | estimated | unknown],
  per_profile: [{profile_id, model?, provider?, …same optional totals}]
}
```

Provider-absent metrics stay `null`/`unknown` — never fabricated. No new
accounting is implemented in I8A.

### 2.9 Request Ledger coverage for interaction mutations

Every interaction mutation (`goal.answer`, `goal.approve`,
`goal.request_changes`, `goal.reject`, `goal.intervene`, `goal.pause`,
`goal.resume`, `goal.cancel`) is wrapped:

```
key   = "ipc-" + envelope.idempotency_key
hash  = sha256(command + canonical payload)
try_claim(key, hash) →
   Some(token): execute business effect in service tx → complete_claim(result)
   None + stored result: return it with status=duplicate (no second effect)
   hash mismatch: Conflict
```

`IpcRequestContext {request_id, idempotency_key, client_pid}` is threaded from
the envelope via a new default-implemented `IpcCommandHandler::handle_request`
method; legacy handlers and tests keep compiling against `handle_command`.

### 2.10 Migration 030 (additive only)

```
030_interaction.sql
  ALTER TABLE approval_requests ADD COLUMN response_json TEXT;
  ALTER TABLE approval_requests ADD COLUMN request_id TEXT;
  ALTER TABLE approval_requests ADD COLUMN source TEXT NOT NULL DEFAULT 'system';
  CREATE TABLE user_interventions (…§2.2B…);
  CREATE UNIQUE INDEX idx_goal_events_goal_seq ON goal_events(goal_id, sequence_num);
  (+ indexes on user_interventions(goal_id, state))
```

Migrations 001–029 untouched.

---

## Part 3 — IPC surface after I8A

| Command | Mutation | Ledgered | Notes |
|---|---|---|---|
| `goal.snapshot` | no | — | **NEW** — GoalSnapshot DTO |
| `goal.events` | no | — | EXTEND — `after_sequence` (existing) + `wait_ms` long-poll |
| `goal.answer` | yes | yes | IMPLEMENT (was stub) — clarification answers |
| `goal.approve` | yes | yes | EXTEND — requires `expected_plan_revision_id`, stale guard, activates plan |
| `goal.request_changes` | yes | yes | **NEW** — formal RequestChanges → replan |
| `goal.reject` | yes | yes | EXTEND — stale guard + terminal semantics |
| `goal.intervene` | yes | yes | **NEW** — UserIntervention |
| `goal.pause` / `goal.resume` | yes | yes | EXTEND — idempotent + dispatch gate enforcement |
| `goal.cancel` | yes | yes | REUSE semantics; idempotent replay |
| everything else | — | — | unchanged |

All mutations carry `request_id` + `idempotency_key` (already in the envelope).

---

## Part 4 — TUI Architecture (I8B design, not built in I8A)

### 4.1 Boundary (hard rule)

```
TUI Application (ratatui + crossterm + tokio — all already in the dependency
                 tree via existing crates; no new framework without need)
  ↓ depends ONLY on:
    - harness-core contracts: ipc.rs (envelopes), presentation.rs (DTOs)
    - an IPC client (same named-pipe client the CLI uses)
  ↓
Supervisor IPC
```

The TUI must NOT depend on: `harness-runtime` repositories, SQLite, sqlx,
Planner/Evaluator implementations, scheduler, or any migration. Enforced by
crate dependency direction when `harness-tui` is created in I8B.

### 4.2 TUI state model

Client state = `GoalSnapshot` + fold(`PresentationEvent`s). The TUI keeps:

- `snapshot: GoalSnapshot` (authoritative at `last_event_sequence`)
- `cursor: u64` (last applied sequence)
- render loop: long-poll `goal.events(after=cursor, wait_ms)` → apply → redraw
- reconnect/crash: re-fetch snapshot, resume from its cursor — no local durable
  state, ever.

User input box → parses `/pause`, `/resume`, `/cancel`, `/approve` etc. into
their IPC requests; any other text → `goal.intervene`. Pending questions and
approvals render from `pending_interactions`; submitting an answer is
`goal.answer` with a fresh `request_id` + `idempotency_key` (retry-safe).

### 4.3 Target layout (I8B wireframe)

```
┌──────────────── General Harness ─────────────────┐
│ repo · goal title · state · elapsed · usage       │  ← snapshot.goal + usage
├───────────────────┬───────────────────────────────┤
│ PLAN / TASKS      │ ACTIVITY                      │
│ ✓ task 1          │ Planner · claude · <model>    │  ← tasks[] states
│ ● task 2 (run)    │ Executor · <agent> · <model>  │  ← running_activities
│ ○ task 3          │ Verification · …              │
├───────────────────┴───────────────────────────────┤
│ Conversation / questions / approvals              │  ← pending_interactions
│ ? Q1: Which database should auth use? [1..n]      │    + event feed
├───────────────────────────────────────────────────┤
│ > _                                               │  ← input → intervene/answer/commands
└───────────────────────────────────────────────────┘
```

I8A ships the state model + DTOs only; no widgets.

---

## Part 5 — Modes, security, testing

### 5.1 Interactive vs NonInteractive

- **NonInteractive (default, unchanged)**: `approval_policy` flags false →
  plans auto-activate, no clarification gate (a planner clarification output in
  non-interactive mode is treated as a planning failure with the questions in
  the error, never silently guessed), pause/cancel still available. All I1–I7
  acceptance behavior preserved.
- **Interactive**: set `require_initial_plan_approval = true` on the GoalSpec
  (the future first-run/TUI flow sets this). Enables plan approval gating and
  clarification waiting; interventions work in both modes.

### 5.2 Security

- Interventions/answers are opaque data; stored, digested, surfaced to the
  Planner as clearly-labeled user directives. No component ever passes user
  text to a shell. Execution continues to flow through planning → policy →
  verification exactly as I4–I7.
- IPC remains same-user Named Pipe, remote clients rejected (existing).

### 5.3 Test matrix (I8A targeted)

Interaction FSM: clarification required → WFA; survives restart; duplicate
answer idempotent; answer resumes planning; plan created → awaiting approval;
no dispatch before approval; approve exact revision; stale approval rejected;
duplicate approve idempotent; request_changes → new revision + old approval
obsolete; intervention durable + restart-safe; pause durable + gates dispatch;
resume continues; duplicate pause/resume safe; cancel reuses production path.

Concurrency: approve vs concurrent revision (stale guard wins); pause vs
dispatch (gate after commit); intervention vs result commit (durable ordering);
same request_id twice → one effect; two different messages → two rows.

Crash windows: after ClarificationRequest commit; after Answer commit before
replan; after PlanApprovalRequest; after Approve before dispatch; after Pause;
after UserIntervention — recovery must show no duplicate effects, no lost
messages, no auto-approval.

IPC: snapshot correctness; events resume-from-sequence; reconnect
(snapshot+events) consistency; long-poll returns promptly on new events.

Quality gates: `cargo fmt --all --check`, `cargo clippy --workspace
--all-targets -- -D warnings`, `cargo test --workspace`, `cargo build
--workspace`. Disk: `CARGO_TARGET_DIR=E:\General-harness\target\scratch`,
`CARGO_INCREMENTAL=0`, temp under `.scratch`.

---

## Part 6 — Explicit non-goals for I8A

Full ratatui UI, installers, release pipelines, first-run wizard, secret
store, provider token accounting beyond the DTO contract, model-assisted
intervention classification, push-based IPC subscriptions, I8B work.
