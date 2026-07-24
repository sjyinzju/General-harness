# I5 Final Report: Controlled Commit and Integration Queue

**Date**: 2026-07-24
**Code Candidate HEAD**: `80533fb3833026a8cbff6547cc59ac2d67584f75`
**Evidence Bundle**: `verification/i5-final-80533fb-20260724-212128/`

---

## Phase Summary

| Phase | Status | Description |
|-------|--------|-------------|
| I5.1  | PASS   | ApprovedCandidate Admission + Controlled Commit |
| I5.2  | PASS   | Durable Integration Queue + Lease/Fencing |
| I5.3  | PASS   | Sandboxed Integration + Verification + Atomic Publish |
| I5.4  | PASS   | Recovery + CLI + Real Git E2E + Final Report |

---

## Defined

### Domain Types (harness-core)

| Type | Location | Status |
|------|----------|--------|
| `CommitRequest` | `contracts/commit.rs` | defined |
| `CommitCandidate` | `contracts/commit.rs` | defined |
| `CommitState` | `contracts/commit.rs` | defined |
| `CommitAdmission` | `contracts/commit.rs` | defined |
| `GitIdentity` | `contracts/commit.rs` | defined |
| `CommitFsm` | `state_machine/commit_fsm.rs` | defined |
| `IntegrationRequest` | `contracts/integration.rs` | defined |
| `IntegrationAttempt` | `contracts/integration.rs` | defined |
| `IntegrationState` | `contracts/integration.rs` | defined |
| `IntegrationStrategy` | `contracts/integration.rs` | defined |
| `IntegrationResult` | `contracts/integration.rs` | defined |
| `IntegrationVerificationPolicy` | `contracts/integration.rs` | defined |
| `VerificationCommand` | `contracts/integration.rs` | defined |
| `ConflictInfo` | `contracts/integration.rs` | defined |
| `IntegrationFsm` | `state_machine/integration_fsm.rs` | defined |

### Persistence (SQLite)

| Table | Migration | Status |
|-------|-----------|--------|
| `commit_requests` | 024 | defined |
| `commit_candidates` | 024 | defined |
| `commit_creation_attempts` | 024 | defined |
| `commit_events` | 024 | defined |
| `integration_requests` | 025 | defined |
| `integration_attempts` | 025 | defined |
| `integration_leases` | 025 | defined |
| `integration_results` | 025 | defined |
| `integration_verifications` | 025 | defined |
| `integration_events` | 025 | defined |

### Services (harness-runtime)

| Service | Location | Status |
|---------|----------|--------|
| `ControlledCommitService` | `commit/service.rs` | defined |
| `CommitRepo` | `commit/repo.rs` | defined |
| `IntegrationQueueService` | `integration/service.rs` | defined |
| `IntegrationExecutor` | `integration/executor.rs` | defined |
| `IntegrationRepo` | `integration/repo.rs` | defined |

### CLI Commands (harness-cli)

| Command | Status |
|---------|--------|
| `harness integration enqueue` | defined |
| `harness integration run-next` | defined |
| `harness integration show` | defined |
| `harness integration list` | defined |
| `harness integration cancel` | defined |
| `harness integration recover` | defined |

---

## Persisted

All ten tables from migrations 024-025 are persisted. States and events use SQLite transactions with idempotency keys. Append-only events are written with INSERT OR IGNORE.

Commit states: `requested`, `materializing`, `created`, `blocked`, `failed`, `cancelled`.
Integration states: `queued`, `waiting_for_lease`, `preparing`, `applying`, `verifying`, `ready_to_publish`, `integrated`, `conflict`, `blocked`, `failed`, `cancelled`, `stale`.

---

## Production Reachable

- `ControlledCommitService` is constructable from `SqlitePool`
- `IntegrationQueueService` is constructable from `SqlitePool`
- `IntegrationExecutor` is constructable with a pool and integration root path
- CLI commands are wired through `dispatch_command` → `dispatch_integration`
- All CLI paths usable via `harness integration <subcommand>` with flags

---

## Unit Tested

### Commitment
- Admission: non-approved review → Blocked
- Admission: stale candidate (tree hash mismatch) → Stale
- Admission: diff digest mismatch → Stale
- Admission: reviewer == executor → Blocked
- Admission: valid candidate → Admitted
- Idempotency: duplicate create → same commit OID
- Stability: same tree/parent/message → stable OID
- Integrity: user index not modified
- Integrity: worktree not modified
- Recovery: Git object before DB write → recoverable
- Scoping: same candidate+review+ref → one CommitCandidate

### Queue
- Enqueue idempotent
- Priority ordering (high > medium > low)
- FIFO tie-breaking (same priority)
- Different repos → parallel
- Different target refs → parallel
- Same repo/ref → serialized
- Empty queue → None
- Invalid target refs rejected
- Duplicate scope returns existing
- Cancel queued

### Integration E2E
- Fast-forward: target unchanged → publish success
- Cherry-pick: target advanced, no conflict → publish success
- Conflict: same file changed → Conflict state, no publish
- Verification failure → Failed state, target ref unchanged
- CAS race: target advanced during execution → CAS fail, no overwrite
- Worktree isolation: integration worktree under managed root

### FSM
- Commit FSM: all legal transitions, terminal immutability
- Integration FSM: full path to Integrated, conflict/stale branches, terminal immutability

---

## Real Git E2E Tested

All integration E2E tests use real `git init`, `git commit`, `git cherry-pick`, and `git update-ref` commands against temporary repositories. No fake git or mock. Covers:

- E2E A: Normal fast-forward integration
- E2E B: Target advanced, no conflict
- E2E C: Conflict detection and recording
- E2E D: Verification failure blocking publish
- E2E E: CAS race protection
- E2E F: Crash recovery (commit object before DB write)

---

## Recovery Tested

- Commit object exists in Git but DB record deleted → recovered via `recover_or_create`
- Response-lost retry → existing commit candidate returned
- Duplicate create → idempotent, one logical CommitCandidate

---

## Quality Gates

| Gate | Result |
|------|--------|
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace` | PASS (all suites, 0 failures) |

---

## Non-Goals

The following are explicitly NOT implemented as specified:

- GitHub Pull Request auto-creation
- `git push`
- Remote CI
- Auto-deploy
- LLM auto-merge-conflict resolution
- Auto re-review
- Multi-reviewer
- Multi-repo distributed transactions
- Supervisor / IPC / Goal Loop / Global Replanning

No modifications were made to:
- I4.5 CompletionEligibility core semantics
- I4.6 Review Decision Policy
- ProcessManager core state machine
- Windows Job Object normal termination
- ResourceClaim base protocol
- Workspace ownership base protocol
- Agent Adapter general protocol

---

## Controlled Commit Details

- Uses `git commit-tree` with explicit GIT_AUTHOR_* and GIT_COMMITTER_* environment variables
- Never modifies global git config (`GIT_CONFIG_NOSYSTEM=1`)
- Commit message includes Harness-Candidate, Harness-Review, Harness-Task, Harness-Execution, Harness-Diff-Digest trailers
- Tree == CandidateSnapshot.candidate_tree_hash
- Parent == CandidateSnapshot.base_commit
- Idempotency key: `commit-{candidate_id}-{review_id}-{target_ref}`
- Deterministic: same inputs → same commit OID

## Integration Details

- Integration worktrees at: `target/harness-integration/<integration-id>/<attempt-id>/`
- Strategies: FastForward (target unchanged), CherryPick (target advanced, no conflict), Conflict
- Publish: `git update-ref <target-ref> <new-head> <expected-old-head>` (atomic CAS)
- Verification: configurable command list with timeout and output limits
- Lease: per (repository_id, target_ref) serialization via UNIQUE INDEX on active leases
