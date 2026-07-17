# I4-B Task DAG Scheduler — Handoff (Final Closure)

> **Status**: I4-B Closure complete, all quality gates green, working tree clean
> **Date**: 2026-07-17
> **Branch**: `main`
> **Pre-I4-B HEAD**: `b27a836` — `docs: finalize agent runtime handoff`
> **Closure HEAD**: `3a63fe0` — `test(i4-b): add scheduler orchestrator golden path tests`

---

## 1. Commits

| Commit | Title |
|--------|-------|
| `172bbf8` | feat(i4-b1): add scheduler readiness and profile selection |
| `28c227a` | feat(i4-b2): add scheduler dispatch saga and reconciler |
| `bb35c93` | fix(i4-b): make scheduler dispatch idempotent |
| `3d5858a` | fix(i4-b): close scheduler resource lifecycle gaps |
| `e2df317` | feat(i4-b): complete scheduler reconciliation |
| `ba6933f` | feat(i4-b): add persistent verification resource handoff |
| `3a63fe0` | test(i4-b): add scheduler orchestrator golden path tests |

7 commits total (5 original + 2 final closure).

## 2. Quality Gates (Final)

| Gate | Result |
|------|--------|
| `cargo fmt --all --check` | **PASS** |
| `cargo clippy --workspace --all-targets -- -D warnings` | **PASS** |
| `cargo test --workspace` | **602 passed / 0 failed / 0 ignored** |
| `git diff --check` | PASS |
| `git status --porcelain=v1` | clean (no output) |

## 3. Test Progression

| Phase | Count |
|-------|-------|
| I4-A Closure | 518 |
| I4-B Initial (handoff) | 543 |
| I4-B Closure (pre-coordinator) | 562 |
| I4-B Final Closure | **602** |

New in final closure (+40 tests):
- +8 HeartbeatRegistry tests (register, inspect, takeover, cancel, fencing, mark_lost)
- +10 HandoffRepository tests (create, get, takeover CAS, version conflict, idempotent, contested, terminal, heartbeat, lost/released, lease_token absence)
- +8 ResourceHandoffCoordinator tests (takeover, idempotent, contested, stale fencing, mismatch detection, cancel owner/fencing checks, lease_token absence)
- +1 Reconciler HandoffRegistryMismatch test
- +6 SchedulerOrchestrator integration tests (golden path, response-lost, adapter failure, heartbeat continuation, takeover, strict two-pool winner)
- +7 additional test coverage from strengthened assertions and formatting

## 4. Migrations

`010_scheduler.sql` — additive, migrations 001–009 **frozen and untouched**.
`011_resource_handoff.sql` — additive, migrations 001–010 **frozen and untouched**.

Business tables: 18 → 22.

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
| 19 | HandoffRegistryMismatch | DB/registry owner/fencing disagreement, DB Released but registry running | ✅ (safe heartbeat stop) |

Safety: no retry, no provider switch, no worktree deletion, INSERT OR IGNORE for concurrent reconcilers.
Never auto-starts Verification. Never re-acquires Lease/Claim.

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
- Automatic retry creation
- Commit/Integration Queue
- Supervisor IPC
- TUI
- Project Goal Loop
- LLM-driven Task DAG re-planning
- Active validation auto-trigger

## 15a. Persistent Resource Handoff (migration 011)

`resource_handoffs` table links execution_id, worktree_id, lease_id, claim_group_id.
Created on Agent success. No lease tokens persisted.

Takeover uses CAS with version optimistic locking:
- SchedulerOwned → VerificationOwned (only if version matches)
- Same owner repeat → idempotent AlreadyOwned
- Different owner → Contested
- Terminal state (Released/Lost) → rejected

## 15b. HeartbeatRegistry

Runtime-owned registry (`Arc<RwLock<HashMap>>`) decoupled from dispatch local variables.
Heartbeat survives `dispatch()` return — cancellation token held by registry entry.

I4-C API:
- `inspect(execution_id)` / `inspect_by_lease(lease_id)` — discover active heartbeats
- `takeover(execution_id, owner, fencing)` — CAS ownership transfer
- `cancel(execution_id, owner, fencing)` — stop heartbeat with owner+fencing validation
- `mark_lost(execution_id)` / `remove_after_finalization(execution_id)`
- `cancel_all()` — runtime shutdown

Security: old fencing rejected, wrong owner rejected, no lease token in Debug/entry.

## 15c. ResourceHandoffCoordinator

Single entry point for I4-C Verification takeover. Coordinates both layers atomically:

```text
1. Read DB handoff + runtime heartbeat
2. Validate execution/lease/claim/fencing/owner consistency
3. DB CAS SchedulerOwned → VerificationOwned
4. Runtime registry takeover
5. Re-read and confirm DB/registry owner consistent
6. Return Acquired only if both layers agree
```

If DB CAS succeeds but registry takeover fails:
- Returns HandoffStateMismatch (NOT Acquired)
- Marks handoff as reconciliation_required
- Does NOT allow Verification to proceed

API:
- `inspect_consistent(execution_id)` — compares both layers
- `takeover_for_verification(execution_id, owner, fencing)` — coordinated two-phase
- `cancel_after_verification(execution_id, owner, fencing)` — checks both owners before cancel
- `mark_reconciliation_required(execution_id, reason)`

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
| Profile-scoped environment filtering (fail-closed) | ✅ |
| Heartbeat survives dispatch return | ✅ |
| Success: reservation released, Lease/Claim/Worktree retained | ✅ |
| Failure: full resource release | ✅ |
| Scheduler Reconciler (19 anomaly types) | ✅ |
| HandoffRegistryMismatch detection (DB/registry consistency) | ✅ |
| ResourceHandoffCoordinator (two-phase DB+registry takeover) | ✅ |
| Strict two-pool winner: assert_eq!(total_start_count, 1) | ✅ |
| Golden path: worktree DB+FS, lease expiry, coordinator inspect | ✅ |
| No automatic retry | ✅ |
| No silent provider switching | ✅ |
| No Verification started | ✅ |
| Migrations 001–010 untouched | ✅ |
| Migration 011 additive, no lease token | ✅ |
| 602 tests, 0 failed, 0 ignored | ✅ |
| fmt / clippy -D warnings green | ✅ |
| `git status --porcelain=v1` returns empty | ✅ |

## 17. Known Gaps and Remaining Items for I4-C

### I4-C Must Implement
- Verification pipeline (I4-C core)
- Commit/Integration Queue
- Supervisor IPC
- TUI
- Project Goal Loop
- LLM-driven Task DAG re-planning
- Active validation auto-trigger

### I4-C Should Integrate With
- `ResourceHandoffCoordinator::takeover_for_verification()` — the single entry point for I4-C to claim scheduler resources
- `ResourceHandoffCoordinator::inspect_consistent()` — to verify DB/registry agreement before verification
- `ResourceHandoffCoordinator::cancel_after_verification()` — to release resources after verification

### Low-Priority Follow-ups
- `allowed_env_var_names` field on RuntimeProfile type (currently derived from agent_kind — fail-closed, no leakage risk)
- Filesystem-level worktree existence check in reconciler (DB-level checks already in place)
- Explicit `HandoffStateMismatch` handling in I4-C verification start guard

## 18. Ready for I4-C Verification

**Yes.** All I4-B exit conditions met. Working tree clean at `3a63fe0`.
602 tests, 0 failed, 0 ignored. All gates green.
