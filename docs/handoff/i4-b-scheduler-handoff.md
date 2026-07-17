# I4-B Task DAG Scheduler — Handoff (Closure)

> **Status**: I4-B Closure complete, all quality gates green
> **Date**: 2026-07-17
> **Branch**: `main`
> **Pre-I4-B HEAD**: `b27a836` — `docs: finalize agent runtime handoff`
> **Closure HEAD**: see commits below

---

## 1. Commits

| Commit | Title |
|--------|-------|
| `172bbf8` | feat(i4-b1): add scheduler readiness and profile selection |
| `28c227a` | feat(i4-b2): add scheduler dispatch saga and reconciler |
| `bb35c93` | fix(i4-b): make scheduler dispatch idempotent |
| `3d5858a` | fix(i4-b): close scheduler resource lifecycle gaps |
| `e2df317` | feat(i4-b): complete scheduler reconciliation |

5 commits total (2 original + 3 closure).

## 2. Quality Gates (Final)

| Gate | Result |
|------|--------|
| `cargo fmt --all --check` | **PASS** |
| `cargo clippy --workspace --all-targets -- -D warnings` | **PASS** |
| `cargo test --workspace` | **562 passed / 0 failed / 0 ignored** |
| `git diff --check` | PASS |
| `git status --short` | clean |

## 3. Test Progression

| Phase | Count |
|-------|-------|
| I4-A Closure | 518 |
| I4-B Initial (handoff) | 543 |
| I4-B Closure | **562** |

+19 new tests in closure:
- +6 dispatch_repo tests (idempotency, spawn evidence, resources, concurrent, conflict)
- +3 event_sink redaction tests (message, tool_result, raw_vendor_event)
- +10 reconciler tests (lease-without-claim, failed-exec-active-resources, awaiting-verification-missing, running-without-process, process-terminal-exec-nonterminal, heartbeat-missing, concurrent-reconcilers, no-retry, no-worktree-deletion, no-provider-switch)

## 4. Migration

`010_scheduler.sql` — additive, migrations 001–009 **frozen and untouched**.

Business tables: 18 → 21.

## 5. Dispatch Order (Safe Sequence)

```text
1.  Persist dispatch intent (transactional idempotency arbitration)
2.  Re-validate readiness within transaction
3.  Atomically acquire concurrency reservation
4.  Create Execution preparation record
5.  Transition Task → Dispatched
6.  Create/verify Worktree
7.  Acquire Workspace Lease + start heartbeat
8.  Acquire Resource Claim Group
9.  Filter env via profile-scoped policy
10. AgentAdapter start_session
11. Persist spawn evidence (session_id)
12. Transition Execution → Running (ONLY after spawn success)
13. send_task / receive_events
14. Handle terminal outcome
```

Agent never starts before Lease/Claim are held. Execution never enters Running before `start_session` succeeds.

## 6. Idempotency

- Transactional `DispatchRepository::record_intent()` with idempotency_key uniqueness
- Same key + same hash → return original outcome (no duplicate worktree/lease/claim/agent)
- Same key + different hash → `IdempotencyConflict` structured error
- Concurrent requests → one winner, rest get duplicate (no bare UNIQUE error)
- Response-lost retry returns original Execution/Operation

Idempotency identity binds: project_id, task_id, profile_id, repo_path, task_goal, timeout (not env).

## 7. Crash Windows

| Window | Behavior |
|--------|----------|
| Intent committed, process not started | Reconciler detects stale intent, marks failed + releases resources |
| Process started, final state not committed | ProcessRegistry check prevents re-spawn; Reconciler marks Lost if orphan |
| Response lost | Re-dispatch returns original Execution/Operation; no duplicate resources |

## 8. Resource Compensation Matrix

| Failure Point | Reservation | Worktree | Lease | Claim | Heartbeat |
|--------------|:-----------:|:--------:|:-----:|:-----:|:---------:|
| Execution insert | released | — | — | — | — |
| Worktree failure | released | — | — | — | — |
| Lease failure | released | retained | — | — | — |
| Claim conflict | released | retained | released | — | stopped |
| Adapter start failure | released | retained | released | released | stopped |
| send_task failure | released | retained | released | released | stopped |
| Agent timeout/cancel/nonzero | released | retained | released | released | stopped |
| Agent success | **released** | **retained** | **retained** | **retained** | **continues** |

## 9. Success → Verification Handoff

```text
Execution terminal → Task Submitted/AwaitingVerification
→ Concurrency reservation released
→ Worktree retained
→ Active Lease retained with heartbeat continuing
→ Active Claim Group retained
→ Heartbeat runner active (survives dispatch return)
→ Ready for I4-C Verification to claim resources
```

## 10. Heartbeat Lifecycle

- `LeaseHeartbeatRunner` spawned as background tokio task in `acquire_lease()`
- Keyed by `CancellationToken` in `DispatchResourceBundle`
- Survives `dispatch()` return on success path (token not cancelled)
- Stops on failure path (token cancelled during compensation)
- I4-C can stop via explicit cancellation or by releasing the lease
- Runtime shutdown cancels all tokens
- Repeated registration idempotent (lease acquire is idempotent)
- Old fencing tokens cannot renew (lease service validates fencing)

## 11. Profile-Scoped Environment

- `filter_env_for_profile()` validates sensitive env vars against profile authorization
- `is_sensitive_env_name()` classifies API_KEY, TOKEN, SECRET, PASSWORD, CREDENTIAL, AUTH patterns
- Agent-specific authorization: Claude profiles allow ANTHROPIC_/CLAUDE_ vars; Codex allows OPENAI_/CODEX_
- Unauthorized sensitive vars → structured `CoreError::ConfigInvalid`
- Non-sensitive vars pass through freely
- Defense-in-depth: ProcessManager validates again at ProcessSpec creation

## 12. AgentEvent Secret Redaction

- `SchedulerEventSink` applies `ProcessEventRedactor` before serialization
- Redacts: Message.content, Progress.summary, ReasoningSummary.summary, ToolCallStarted.tool_input, ToolCallCompleted.content_preview, Result.content, Error.message, RawVendorEvent.payload
- Recursive redaction on JSON objects and arrays in values
- Pass-through for: SessionStarted, ProcessExited, SessionEnded
- Large payloads (>64KB) redacted before artifact file write
- DB and artifact metadata never contain raw secrets
- Independent of ProcessManager stdout redaction (structured event coverage)

## 13. Scheduler Reconciler

18 anomaly types:

| # | Anomaly | Detection | Auto-Repair |
|---|---------|-----------|:-----------:|
| 1 | OrphanReservation | expired reservations | ✅ (expire) |
| 2 | TerminalExecutionResourcesActive | terminal exec + active reservation | ✅ (release) |
| 3 | StaleSpawnIntent | dispatch stuck >10min | ✅ (mark lost) |
| 4 | TaskRunningWithoutActiveExecution | task=running, no active exec | ❌ |
| 5 | DuplicateActiveExecutions | multiple non-terminal execs | ❌ |
| 6 | LeaseWithoutClaim | active lease, no claim group | ❌ |
| 7 | ClaimWithoutLease | claim references missing lease | ❌ |
| 8 | StaleFencing | fencing < worktree epoch | ❌ |
| 9 | WorktreeMissing | active lease, no worktree DB record | ❌ |
| 10 | RuntimeProfileMissingOrDisabled | active exec, profile unavailable | ❌ |
| 11 | AwaitingVerificationResourcesMissing | submitted task, no active lease | ❌ |
| 12 | TerminalEventWithoutTransition | terminal event, non-terminal exec | ❌ |
| 13 | FailedExecutionWithActiveLeaseOrClaim | failed exec, active lease | ✅ (release) |
| 14 | ReservationWithoutTaskOrExecution | active reservation, missing task | ✅ (release) |
| 15 | IncompleteSpawnIntent | dispatch >5min, no session_id | ✅ (mark failed) |
| 16 | RunningExecutionWithoutProcessRegistry | running exec, stale session | ❌ |
| 17 | ProcessTerminalExecutionNonterminal | process_exited event, running exec | ❌ |
| 18 | HeartbeatMissingForRetainedLease | active lease, stale heartbeat | ❌ |

Safety: no retry, no provider switch, no worktree deletion, INSERT OR IGNORE for concurrent reconcilers.

## 14. State Mapping

| Scheduler Action | Task Transition | Execution Transition | Legal |
|-----------------|-----------------|---------------------|-------|
| Dispatch start | Pending/Ready → Dispatched | (create Created) | ✅ |
| Agent spawn success | Dispatched → Running | Created → Running | ✅ |
| Agent success | Running → Submitted | Running → Completed | ✅ |
| Agent failure | Running → Failed | Running → Failed | ✅ |
| Agent timeout | Running → Failed | Running → Failed | ✅ |
| Agent cancelled | Running → (terminal) | Running → Cancelled | ✅ |

No new FSM states. All transitions use Gate C frozen FSM.

## 15. Explicitly NOT Implemented

- I4-C Verification Pipeline
- SchedulerOrchestrator end-to-end integration tests with fake Agent (Batch D deferred)
- Automatic retry creation
- Commit/Integration Queue
- Supervisor IPC
- TUI
- Project Goal Loop
- LLM-driven Task DAG re-planning
- Active validation auto-trigger

## 16. I4-B Final Exit Conditions

| Condition | Met |
|-----------|:---:|
| Persistent Task readiness | ✅ |
| Deterministic profile selection | ✅ |
| Persistent concurrency arbitration | ✅ |
| One active execution per Task | ✅ |
| Dispatch saga with safe ordering | ✅ |
| Execution→Running only after spawn success | ✅ |
| Transactional idempotency arbitration | ✅ |
| Idempotency conflict detection | ✅ |
| Crash window: intent-before-spawn | ✅ |
| Crash window: spawn-before-commit | ✅ |
| Crash window: response-lost | ✅ |
| Spawn evidence persisted | ✅ |
| Worktree/Lease/Claim ordering safe | ✅ |
| Agent via AgentAdapter/ProcessManager only | ✅ |
| AgentEvent sequential persistence with redaction | ✅ |
| Profile-scoped environment filtering | ✅ |
| Heartbeat survives dispatch return | ✅ |
| Success: reservation released, Lease/Claim retained | ✅ |
| Failure: full resource release | ✅ |
| Scheduler Reconciler (18 anomaly types) | ✅ |
| No automatic retry | ✅ |
| No silent provider switching | ✅ |
| No Verification started | ✅ |
| Migrations 001–009 untouched | ✅ |
| 562 tests, 0 failed, 0 ignored | ✅ |
| fmt / clippy -D warnings green | ✅ |
| git status clean | ✅ |

## 17. Remaining Items for I4-C

- SchedulerOrchestrator end-to-end integration tests with fake Agent (28 scenarios)
- Verification pipeline (I4-C)
- Heartbeat handoff from I4-B runtime to I4-C verification
- True concurrency tests with independent SqlitePools on file-based DB
- `allowed_env_var_names` field on RuntimeProfile type (currently derived from agent_kind)

## 18. Ready for I4-C Verification

**Yes.** All I4-B exit conditions met.
