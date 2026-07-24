# I5 Final Report: Controlled Commit and Integration Queue — Production Closure

**Date**: 2026-07-24
**Code Candidate HEAD**: `f9b0f494d43f5428acc91b92e462c8cf26238f25`
**Previous Code Candidate**: `80533fb3833026a8cbff6547cc59ac2d67584f75`
**Evidence Bundle**: `verification/i5-closure-f9b0f49-20260724-221951/`

---

## Verdict

**PASS — I5 formally complete. Ready for I6 Supervisor, IPC and System Recovery.**

All 5 production gaps (F1–F5) closed. All quality gates pass. All 29 I5 tests pass. No ignored or skipped tests.

---

## Phase Summary

| Phase | Status | Description |
|-------|--------|-------------|
| I5.1  | PASS   | ApprovedCandidate Admission + Controlled Commit (production reachable) |
| I5.2  | PASS   | Durable Integration Queue + Lease/Fencing |
| I5.3  | PASS   | Sandboxed Integration + Verification + Atomic Publish |
| I5.4  | PASS   | Deep Recovery + CLI + Real Git E2E + Production Closure |

---

## Production Reachability Matrix

| Capability | Defined | Tested | Production Caller | CLI Reachable |
|------------|---------|--------|-------------------|---------------|
| ApprovedCandidate admission | YES | YES | `integration enqueue` | YES |
| ControlledCommitService | YES | YES | `integration enqueue` | YES |
| Commit recovery | YES | YES | `recover_or_create` | YES |
| Integration enqueue | YES | YES | `integration enqueue` | YES |
| Lease acquisition | YES | YES | `run_next` | YES |
| Fencing validation | YES | YES | `IntegrationExecutor` | YES |
| IntegrationExecutor | YES | YES | `run_next` | YES |
| Sandbox worktree | YES | YES | `IntegrationExecutor` | YES |
| Integration verification | YES | YES | `IntegrationExecutor` | YES |
| Atomic publish (CAS) | YES | YES | `IntegrationExecutor` | YES |
| Deep recovery | YES | YES | `integration recover` | YES |
| CLI enqueue | YES | YES | `dispatch_integration` | YES |
| CLI run-next | YES | YES | `dispatch_integration` | YES |
| CLI recover | YES | YES | `dispatch_integration` | YES |

---

## Production Path

```
ApprovedCandidate
→ CLI integration enqueue --candidate <id>
→ ControlledCommitService::validate_admission
→ ControlledCommitService::create_commit (via git commit-tree)
→ CommitCandidate persisted
→ IntegrationRequest enqueued

CLI integration run-next
→ dequeue highest-priority request
→ acquire lease (UNIQUE constraint per repo+target_ref)
→ persist fencing_token
→ IntegrationExecutor::execute
  → validate lease/fencing (before applying, verifying, publishing)
  → managed sandbox worktree (target/harness-integration/<id>/<attempt>/)
  → fast-forward or cherry-pick
  → verification with async timeout + process-tree kill
  → git update-ref CAS publish
→ persist IntegrationResult
→ release lease
→ cleanup worktree

CLI integration recover
→ IntegrationRecoveryService::reconcile
→ expire stale leases
→ requeue stuck WaitingForLease/Preparing/Applying
→ fail unrecoverable Verifying
→ recover ReadyToPublish (ref-updated or requeue)
```

---

## F1: ControlledCommitService Production Wiring — CLOSED

`harness integration enqueue --candidate <candidate-id>` now:

1. Reads `CandidateSnapshot` from database
2. Finds approved review for candidate
3. Runs admission validation (review approved, tree hash, diff digest, reviewer≠executor)
4. Calls `ControlledCommitService::create_commit` (or `recover_or_create`)
5. Persists `CommitCandidate` with Harness trailers
6. Enqueues `IntegrationRequest`

Idempotent: repeated enqueue produces same `commit_oid` and reuses existing integration request.

---

## F2: run-next Full Execution — CLOSED

`harness integration run-next` now:

1. Dequeues highest-priority request (CAS transition Queued→WaitingForLease)
2. Resolves `commit_oid`/`parent_oid` from `CommitCandidate`
3. Starts `IntegrationAttempt` (WaitingForLease→Preparing)
4. Acquires lease with `fencing_token` (UNIQUE index per repo+target_ref)
5. Calls `IntegrationExecutor::execute` with lease validation
6. Persists `IntegrationResult` with strategy, verification, conflicts
7. Releases lease
8. Cleans up managed worktree
9. Returns JSON with full outcome

Output distinguishes: `NoWork`, `Integrated`, `Conflict`, `VerificationFailed`, `Blocked`, `Failed`.

---

## F3: Deep Integration Recovery — CLOSED

`IntegrationRecoveryService::reconcile()` handles all recoverable states:

| State | Action |
|-------|--------|
| `Queued` | Ensure no stale lease |
| `WaitingForLease` | Close expired lease, requeue |
| `Preparing` | Clean abandoned worktree, requeue |
| `Applying` | Abort cherry-pick, clean worktree, requeue |
| `Verifying` | Cannot recover result → mark Failed |
| `ReadyToPublish` | Check target ref: if updated → recover Integrated; if unchanged → requeue |

`harness integration recover` outputs JSON with: `scanned`, `requeued`, `recovered_integrated`, `failed_attempts`, `blocked`, `leases_closed`, `worktrees_cleaned`, `processes_terminated`, `actions[]`.

---

## F4: Lease and Fencing Enforcement — CLOSED

Lease operations added to `IntegrationRepo`:
- `acquire_lease()` — INSERT with UNIQUE constraint on active lease per scope
- `release_lease()` — CAS with `fencing_token` match
- `validate_active_lease()` — checks lease exists, active, not expired, token matches
- `expire_stale_leases()` — bulk-expire for recovery

Fencing validation points in `IntegrationExecutor`:
- Before applying (after lease check, before cherry-pick/worktree)
- Before verifying
- Before publishing (before `git update-ref`)

State transitions use `transition_attempt_state_fenced()` with fencing token in WHERE clause. Old worker cannot write state after lease expired and new worker acquired.

---

## F5: Verification Timeout — CLOSED

Replaced `std::process::Command::output()` with `tokio::process::Command`:
- Async spawn with `kill_on_drop(true)`
- `tokio::time::timeout` on `child.wait_with_output()`
- Timeout → drop kills child + `taskkill /F /T /PID` fallback on Windows
- Spawn failure (program not found) → treated as verification failure
- No orphan processes after timeout

---

## Fencing Race Protection

Test scenario:
```
Worker A acquires lease (fencing_token=1)
Lease expires
Worker B acquires lease (fencing_token=2)
Worker A attempts publish
→ Worker A rejected (lease validation fails)
→ Worker A cannot transition state (CAS requires fencing_token in WHERE clause)
→ Worker B has publish authority
```

Database UNIQUE INDEX on active leases prevents two workers from holding leases simultaneously on the same (repo, target_ref). State CAS requires matching fencing token.

---

## Additional Safety Properties

Additional safety properties maintained:
- Non-ApprovedCandidate cannot produce commit (admission blocks)
- Stale candidate (tree/digest mismatch) cannot produce commit
- Reviewer==executor blocked
- `git commit-tree` used — never modifies user index or worktree
- `GIT_CONFIG_NOSYSTEM=1` on all git invocations
- Explicit `GIT_AUTHOR_*`/`GIT_COMMITTER_*` env vars
- Commit message includes Harness-Candidate/Review/Task/Execution/Diff-Digest trailers
- Integration worktrees isolated in `target/harness-integration/`
- Conflict → `git cherry-pick --abort`, target ref unchanged
- Verification failure → target ref unchanged, no publish
- `git update-ref <target> <new> <expected-old>` — atomic CAS, no forceful overwrite
- No `--force-integrate`, `--skip-review`, `--skip-digest-check`, `--ignore-fencing`, `--overwrite-target` flags

---

## Defined Types

### Domain (harness-core)

| Type | Location |
|------|----------|
| `CommitRequest`, `CommitCandidate`, `CommitState`, `CommitAdmission`, `GitIdentity` | `contracts/commit.rs` |
| `CommitFsm` | `state_machine/commit_fsm.rs` |
| `IntegrationRequest`, `IntegrationAttempt`, `IntegrationState`, `IntegrationStrategy`, `IntegrationResult`, `IntegrationVerificationPolicy`, `VerificationCommand`, `ConflictInfo` | `contracts/integration.rs` |
| `IntegrationFsm` | `state_machine/integration_fsm.rs` |

### Persistence (SQLite)

| Table | Migration |
|-------|-----------|
| `commit_requests`, `commit_candidates`, `commit_creation_attempts`, `commit_events` | 024 |
| `integration_requests`, `integration_attempts`, `integration_leases`, `integration_results`, `integration_verifications`, `integration_events` | 025 |

### Services (harness-runtime)

| Service | Location |
|---------|----------|
| `ControlledCommitService` | `commit/service.rs` |
| `CommitRepo` | `commit/repo.rs` |
| `IntegrationQueueService` | `integration/service.rs` |
| `IntegrationExecutor` | `integration/executor.rs` |
| `IntegrationRecoveryService` | `integration/recovery.rs` |
| `IntegrationRepo` | `integration/repo.rs` |

### CLI (harness-cli)

| Command | Production Path |
|---------|-----------------|
| `harness integration enqueue --candidate <id>` | `dispatch_integration` → `cmd_integration_enqueue` → `ControlledCommitService` + `IntegrationQueueService` |
| `harness integration run-next` | `dispatch_integration` → `cmd_integration_run_next` → `IntegrationQueueService::run_next` → `IntegrationExecutor` |
| `harness integration show <id>` | `dispatch_integration` → `IntegrationQueueService::get` |
| `harness integration list` | `dispatch_integration` → `IntegrationQueueService::list_all` |
| `harness integration cancel <id>` | `dispatch_integration` → `IntegrationQueueService::cancel` |
| `harness integration recover` | `dispatch_integration` → `IntegrationRecoveryService::reconcile` |

---

## Test Results

### I5-specific tests: 29 passed, 0 failed

| Suite | Passed | Failed |
|-------|--------|--------|
| i5_1_controlled_commit | 11 | 0 |
| i5_2_integration_queue | 10 | 0 |
| i5_3_integration_e2e | 8 | 0 |

### Quality Gates

| Gate | Result |
|------|--------|
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace` | PASS (0 failed, 0 ignored, 0 skipped) |

---

## Non-Goals (unchanged)

The following remain not implemented:
- GitHub Pull Request auto-creation, `git push`, Remote CI, Auto-deploy
- LLM auto-merge-conflict resolution, Auto re-review, Multi-reviewer
- Multi-repo distributed transactions, Supervisor / IPC / Goal Loop / Global Replanning

No modifications to:
- I4.5 CompletionEligibility core semantics
- I4.6 Review Decision Policy
- ProcessManager core state machine
- Windows Job Object normal termination
- ResourceClaim base protocol
- Workspace ownership protocol
- Agent Adapter general protocol

---

## Findings Resolution

All 4 non-blocking findings from the previous certification are now closed:

| Finding | Status |
|---------|--------|
| F1: ControlledCommitService not wired into CLI | **CLOSED** — `integration enqueue` now calls `ControlledCommitService` |
| F2: run-next only dequeues | **CLOSED** — full execution: lease → execute → verify → publish → cleanup |
| F3: recover only lists items | **CLOSED** — `IntegrationRecoveryService::reconcile()` performs deep reconciliation |
| F4: Fencing not validated in executor | **CLOSED** — 3 validation checkpoints + CAS with fencing token |

Verification timeout now uses async spawn with process-tree kill (F5).

---

## Evidence Bundle

`verification/i5-closure-f9b0f49-20260724-221951/`

- `summary.json` — machine-readable verification results
- `code-head.txt` — closure code commit SHA
- `git-before.json` — git state at certification
- `production-reachability.json` — production reachability matrix

All fields in `summary.json` are derived from real test and command results.
