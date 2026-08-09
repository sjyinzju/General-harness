# I8A Final Report — Human Interaction Protocol + TUI Architecture Foundation

**Date**: 2026-08-09
**Baseline START_HEAD**: `c5c7f1cdd5c07bb7f2211d97633006fe9f9da316` (I1–I7 sealed)
**Design document**: `docs/I8A_INTERACTION_ARCHITECTURE.md` (500 lines)

---

## Verdict

**PASS — I8A formally complete. The durable Human Interaction domain,
Request-Ledger IPC, and snapshot/event presentation layer are in place. I1–I7
behavior in non-interactive mode is unchanged.**

---

## Deliverables

### A. Interaction domain (`feat(interaction)`)

| Artifact | Path | Lines |
|----------|------|-------|
| Migration 030 — additive schema | `migrations/030_interaction.sql` | 43 |
| `user_interventions` table | migration 030 | — |
| `approval_requests` extensions (`response_json`, `request_id`, `source`) | migration 030 | — |
| Atomic goal-event sequencing (`UNIQUE goal_id, sequence_num`) | migration 030 | — |
| Interaction service (`GoalLoopService` impl block) | `src/goal/interaction.rs` | 656 |
| Repo methods (`insert_intervention`, `list_interventions`, `cancel_pending_approvals`, `resolve_approval_with_response`, `update_plan_state`, …) | `src/goal/repo.rs` | +256 |
| Service wiring (`request_plan_approval`, `approve_plan`, `reject_plan`, `request_plan_changes`, `record_intervention`, `pause_goal`, `resume_goal`) | `src/goal/service.rs` | +240 |
| Planner outcome `ClarificationNeeded` | `src/goal/planner.rs` | +72 |
| Types re-export (`UserIntervention`, `InterventionClassification`, `InterventionState`) | `src/goal/mod.rs` | +120 |
| FSM tests (25 cases: clarification, plan approval, stale guards, request-changes, interventions, pause/resume, dispatch gates, event sequence) | `tests/interaction_protocol.rs` | 939 |

### B. IPC layer (`feat(ipc)`)

| Artifact | Path | Lines |
|----------|------|-------|
| Presentation DTOs (`GoalSnapshot`, `PresentationEvent`, `UsageSummary`, …) | `harness-core/src/contracts/presentation.rs` | 167 |
| New IPC commands (`GoalSnapshot`, `GoalRequestChanges`, `GoalIntervene`) | `harness-core/src/contracts/ipc.rs` | — |
| `IpcRequestContext` + `IpcHandlerOutcome` + `handle_request` default method | `src/ipc/mod.rs` | — |
| Supervisor ledger wrapper (try_claim → execute → complete/duplicate/conflict) | `src/supervisor/command_handler.rs` | — |
| Hash-bound replay (sha256 of command + canonical payload) | `src/supervisor/command_handler.rs` | — |
| Completed-key hash guard (same key, different payload → Conflict) | `src/supervisor/command_handler.rs` | — |
| `goal.intervene` request_id injection (hash stays on client payload) | `src/supervisor/command_handler.rs` | — |
| 9 command implementations: pause/resume/cancel/approve/reject/answer/request_changes/intervene/snapshot | `src/supervisor/command_handler.rs` | — |
| `goal.events` long-poll (wait_ms ≤ 30s, 100ms poll interval) | `src/supervisor/command_handler.rs` | — |
| `goal.snapshot` one-pass projection (goal + plan + tasks + pending + interventions + running + usage + cursor) | `src/supervisor/command_handler.rs` | — |
| IPC integration tests (17 cases: snapshot, events resume, long-poll, reconnect gapless, ledger replay, conflict, in-flight, intervene request_id, approve/reject/request_changes/answer/cancel over IPC) | `tests/ipc_interaction_tests.rs` | 882 |

### C. Documentation (`docs(i8a)`)

| Artifact | Path | Lines |
|----------|------|-------|
| Architecture document (audit + design + IPC surface + test matrix) | `docs/I8A_INTERACTION_ARCHITECTURE.md` | 500 |
| Final report | `verification/I8A_FINAL_REPORT.md` | this |

---

## Quality gates

| Gate | Command | Result |
|------|---------|--------|
| Format | `cargo fmt --all --check` | **PASS** |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | **PASS** (0 warnings) |
| Test | `cargo test --workspace --no-fail-fast` | **PASS** (all targets green, 0 failed) |
| Build | `cargo build --workspace` | **PASS** |

Test census: **25** FSM tests in `interaction_protocol.rs` + **17** IPC tests in
`ipc_interaction_tests.rs` = **42** new interaction tests. Existing table-count
census tests updated (81 → 82 business tables after migration 030).

---

## Request-Ledger semantics (verified)

| Scenario | Expected | Verified |
|----------|----------|----------|
| First request with key K | `Success` + side effect applied | ✅ |
| Retry with same K + same payload (new `request_id`) | `Duplicate` + stored result replayed | ✅ |
| Retry with same K + **different** payload (completed) | `Conflict` (hash mismatch) | ✅ |
| Retry with same K + different payload (pending) | `Conflict` (`idempotency_request_mismatch`) | ✅ |
| Retry with same K + same payload (pending, no result) | `Accepted` `in_flight` | ✅ |
| Empty `idempotency_key` | Ledger bypassed, service-level idempotency | ✅ |
| Read-only commands (`GoalSnapshot`, `GoalEvents`) | Never ledgered | ✅ |

---

## First-principle compliance

1. **TUI is never a second business writer.** All interaction mutations flow
   TUI → IPC envelope → `IpcRequestContext` → `handle_request` (ledger wrap) →
   `GoalLoopService` → Repository. The TUI/CLI cannot reach business tables.

2. **User natural-language input is data, not commands.** `goal.intervene`
   stores the message + classification in `user_interventions` and appends a
   `user_intervention_recorded` event. Nothing reaches a shell.

3. **Non-interactive mode is unchanged.** When `approval_policy` flags are all
   false (the I1–I7 default), the goal loop never creates an approval request
   and proceeds straight to dispatch. No behavioral regression.

4. **Crash recovery is durable.** Interaction state lives in plain SQLite rows
   + FSM states. The `idempotency_records` claim table backs the request
   ledger: a crash mid-execution leaves a `pending` claim; reconnect returns
   `in_flight`; once completed, the result is replayed exactly.

---

## What I8A does NOT include (by design)

- No full Ratatui TUI (I8B scope)
- No installer or release pipeline
- No secret store
- No push-based IPC subscription (long-poll only)
- No I8B work started

---

## Commits

| Commit | Scope | Message |
|--------|-------|---------|
| A | Interaction domain + migration 030 + FSM tests | `feat(interaction): durable human-interaction domain with idempotent FSM transitions` |
| B | IPC layer + presentation DTOs + ledger wrapper + IPC tests | `feat(ipc): request-ledger interaction commands with snapshot and long-poll events` |
| C | Architecture document + this report | `docs(i8a): interaction architecture and final report` |
