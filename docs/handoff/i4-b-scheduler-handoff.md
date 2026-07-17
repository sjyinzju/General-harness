# I4-B Task DAG Scheduler — Handoff

> **Status**: I4-B complete, quality gates all green
> **Date**: 2026-07-17
> **Branch**: `main`

---

## 1. Commits

| Commit | Title |
|--------|-------|
| `172bbf8` | feat(i4-b1): add scheduler readiness and profile selection |
| `28c227a` | feat(i4-b2): add scheduler dispatch saga and reconciler |

## 2. Quality Gates

| Gate | Result |
|------|--------|
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace` | **543 passed / 0 failed / 0 ignored** |
| `git diff --check` | PASS |
| `git status --short` | clean |

## 3. Test Progression

| Phase | Count |
|-------|-------|
| I3 Final | 399 |
| I4-A Initial | 469 |
| I4-A Closure | 518 |
| I4-B | **543** |

+25 new tests (18 scheduler + 4 event_sink + 3 reconciler).

## 4. Migration

`010_scheduler.sql` — additive, migrations 001–009 frozen:
- `scheduler_reservations` — concurrency slots with UNIQUE(one active per task)
- `dispatch_operations` — saga tracking with idempotency
- `scheduler_reconciliations` — anomaly detection log

Business tables: 18 → 21.

## 5. Task Readiness

`TaskReadinessEvaluator`:
- Queries persisted Task + TaskDependency (never in-memory list)
- 13 readiness states: Ready, Blocked, Terminal, ActiveExecutionExists, NoCompatibleProfile, RequiresProfileValidation, RequiresExplicitClaim, ConcurrencyLimited, AwaitingHuman, DependencyCycle, DependencyMissing, UpstreamFailed
- Cycle detection: DFS with white/gray/black sets, returns cycle path
- Tests: no-dependency-ready, dependency-complete-ready, dependency-incomplete-blocked, dependency-failed, missing-dependency, dependency-cycle, terminal-not-ready, active-execution-blocks

## 6. Profile Selection

`RuntimeProfileSelector`:
- Deterministic ordering: explicit preference → agent-kind filter → priority score (validated > untested > degraded) → alphabetical tie-break
- Explicit preference checked first with early return
- Never guesses capability from model name
- Never treats env-var presence as authenticated
- No silent provider switching
- Tests: explicit-selected, explicit-unavailable, deterministic-tie-break, no-compatible-profile, no-silent-switching

## 7. Concurrency Arbitration

`ConcurrencyManager`:
- Atomic reservation in SQLite transaction
- Global limit, per-profile limit, per-repository limit
- UNIQUE index enforces one active reservation per task
- Idempotent: duplicate reservation returns conflict
- Expiry and reclaim: stale reservations can be expired and re-reserved
- Tests: reserve-success, one-active-per-task, global-limit, per-profile-limit, expired-reclaimed

## 8. Dispatch Saga

`SchedulerOrchestrator` (10-phase flow):
1. Reserve concurrency
2. Create Execution Attempt
3. Transition Task → Dispatched
4. Create Worktree (via WorktreeManager)
5. Acquire Workspace Lease (via WorkspaceLeaseService)
6. Acquire Resource Claims (via ResourceClaimService)
7. Transition Execution → Running
8. Start Adapter session + send TaskEnvelope
9. Receive events via SchedulerEventSink
10. Handle terminal outcome

Compensation:
- Worktree failure → release concurrency, mark execution failed
- Lease failure → release concurrency, mark execution failed
- Claim conflict → release concurrency, fail dispatch
- Adapter failure → release concurrency, mark execution failed

Success: retains Lease + Claim for I4-C Verification.
Failure: releases all resources.

## 9. Event Persistence

`SchedulerEventSink`:
- AgentEvent persistence with execution-scoped sequence numbers
- Large payloads (>64KB) → artifact file references
- Crash-safe: DB failure closes sink, stops further writes
- Sequence ordering guaranteed via AtomicU64
- Tests: persist, sequence-ordered, closed-after-error, raw-vendor-event

## 10. Reconciler

`SchedulerReconciler`:
- Detects: orphan reservations, terminal-execution resource leaks, duplicate active executions, stale spawn intents, expired reservations
- Repairs: auto-releases terminal-execution reservations, expires stale reservations, marks stale spawns as Lost
- Idempotent: repeated reconciliation safe, concurrent reconcilers use INSERT OR IGNORE
- Tests: orphan-reservation-reclaimed, duplicate-active-execution-detected, repeated-reconcile-idempotent

## 11. State Mapping

| Scheduler Action | Task Transition | Execution Transition | Legal |
|-----------------|-----------------|---------------------|-------|
| Dispatch start | Pending/Ready → Dispatched | (create Created) | ✅ |
| Agent start | Dispatched → Running | Created → Running | ✅ |
| Agent success | Running → Submitted | Running → Completed | ✅ |
| Agent failure | Running → Failed | Running → Failed | ✅ |
| Agent timeout | Running → Failed | Running → Failed | ✅ |
| Agent cancelled | Running → (terminal) | Running → Cancelled | ✅ |

No new FSM states added. All transitions use Gate C frozen TaskFsm and ExecutionFsm.

## 12. Explicitly NOT Implemented

- I4-C Verification Pipeline
- Automatic retry creation
- Commit/Integration Queue
- Supervisor IPC
- TUI
- Project Goal Loop
- LLM-driven Task DAG re-planning
- Active validation auto-trigger

## 13. I4-B Exit Conditions

| Condition | Met |
|-----------|:---:|
| Persistent Task readiness | ✅ |
| Deterministic profile selection | ✅ |
| Persistent concurrency arbitration | ✅ |
| One active execution per Task | ✅ (UNIQUE index) |
| Dispatch saga complete | ✅ |
| Worktree/Lease/Claim ordering safe | ✅ |
| Agent via AgentAdapter/ProcessManager only | ✅ |
| AgentEvent sequential persistence | ✅ |
| Response-lost idempotent | ✅ |
| Success retains Lease/Claim for Verification | ✅ |
| Failure releases resources | ✅ |
| Scheduler Reconciler complete | ✅ |
| No automatic retry | ✅ |
| No silent provider switching | ✅ |
| No Verification started | ✅ |
| Migrations 001–009 untouched | ✅ |
| 543 tests, 0 failed, 0 ignored | ✅ |
| fmt/clippy green | ✅ |

## 14. Ready for I4-C Verification

**Yes.** All I4-B exit conditions met.
