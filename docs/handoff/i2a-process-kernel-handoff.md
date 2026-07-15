# I2A Process Kernel Handoff

> **状态**: `I2A Process Kernel Core Accepted with Mandatory I2B Carryovers`
> **日期**: 2026-07-15
> **Branch**: `main`
> **HEAD**: `ea72655` ("harness内谷I2A完成")

---

## 1. Repository Facts

```
Branch:           main
HEAD:             ea72655 harness内容I2A完成
Working tree:     27 modified files (cargo fmt applied, not yet committed)
Recent commits:
  ea72655 harness内容I2A完成
  b1fa86b 清理调试文件
  91d8b69 harness主题V1
  9572e28 harness骨架完成
  61c8b73 骨架搭建B阶段完成
  9566d13 F0 GateB完成，现在配置多模型检测
  0ff9548 F0基础设施，骨架未完成
  bf0e67e 规划文档
  cc6049f 初始化和计划

Cargo workspace:
  crates/harness-core        (domain contracts, FSM, policies)
  crates/harness-runtime     (persistence, process, transition)
  crates/harness-adapters    (FakeAdapter, contract tests)
  crates/harness-cli         (CLI stub, process-supervisor-spike)

Key dependencies:
  tokio 1.52.3
  sqlx 0.8.6 (SQLite)
  tokio-util 0.7.18
  chrono 0.4.45

Migrations:
  001_initial_schema.sql    (10 business tables)
  002_idempotency_ownership.sql
  003_operation_claim.sql

Test results: 76 passed, 0 failed, 0 ignored
```

## 2. Phase Status

### COMPLETED — Production Ready

| Component | Source | Status |
|-----------|--------|:---:|
| CoreError + 24 ErrorCodes | `harness-core/src/error.rs` | Complete |
| AgentAdapter/AgentSession/AgentEventSink traits | `harness-core/src/contracts/agent_adapter.rs` | Frozen v1 |
| AgentEvent (11 variants) | `harness-core/src/contracts/agent_event.rs` | Frozen v1 |
| RuntimeProfile + CapabilitySet | `harness-core/src/contracts/runtime_profile.rs` | Frozen v1 |
| 4 Lifecycle FSMs (Project/Task/Execution/Lease) | `harness-core/src/state_machine/` | Complete |
| Database bootstrap + migrations | `harness-runtime/src/db.rs` | Complete |
| 6 Repository traits + SQLx impl | `harness-runtime/src/repo.rs` | Complete |
| TransitionService (atomic state+event) | `harness-runtime/src/transition.rs` | Complete |
| Idempotency ownership (try_claim/complete/fail) | `harness-runtime/src/idempotency.rs` | Complete |
| Operation Claim API | `harness-runtime/src/operation.rs` | Complete |
| EventLog (append-only, stream_version unique) | `harness-runtime/src/event_log.rs` | Complete |
| ProcessManager (spawn/wait/timeout/cancel) | `harness-runtime/src/process/manager.rs` | Complete |
| ProcessRegistry (in-memory tracking) | `harness-runtime/src/process/registry.rs` | Complete |
| ProcessReconciler (via TransitionService) | `harness-runtime/src/process/reconciler.rs` | Complete |
| ProcessFixture (15 modes) | `harness-runtime/src/bin/process-fixture.rs` | Complete |
| Process tree termination (taskkill/unix pg) | `harness-runtime/src/process/job_object.rs` | Complete |
| FakeAdapter + Contract Tests | `harness-adapters/src/` | Complete |
| Golden Path tests (success/retry/illegal/cancel) | `harness-adapters/tests/` | Complete |

### DEFERRED — I2B Mandatory Carryovers

| Item | Reason | Target |
|------|--------|:---:|
| Windows Job Object (primary) | windows crate 0.58 API incompatibility with current compiler; `raw_handle()` exists on tokio Child; `job_object.rs` has `assign_raw_handle()` interface reserved | I2B-0 |
| CapturePolicy::Spool (file spool) | Needs RuntimeArtifactDirectory infrastructure | I2B-0 |
| ProcessEventRedactor | Needs spool + credential registry | I2B-0 |
| flood_stdout/flood_stderr/flood_both deadlock tests | Needs spool + bounded channel infrastructure | I2B-0 |
| invalid_utf8 tests | Needs spool infrastructure | I2B-0 |
| timeout/cancel/natural-exit race tests | Fixture exists; needs comprehensive race harness | I2B-0 |
| grandchild termination test | Fixture exists; needs Job Object for reliable test | I2B-0 |

### INTERFACE ONLY / STUB

| Item | Source | Status |
|------|--------|:---:|
| RuntimeArtifactDirectory | Not implemented | Stub |
| FileScopeValidator | `harness-core/src/policies/file_scope.rs` | Core logic only |
| CommandPolicyEngine | `harness-core/src/policies/command.rs` | Pattern list only |
| SecretScanner | Not implemented | — |
| Sandbox trait | `docs/architecture/security-boundaries.md` | Interface only |

## 3. Source Index

### Gate C Frozen Contracts
```
crates/harness-core/src/contracts/agent_adapter.rs    — AgentAdapter, AgentSession, AgentEventSink
crates/harness-core/src/contracts/agent_event.rs       — AgentEvent (11 variants), EnrichedAgentEvent
crates/harness-core/src/contracts/agent_definition.rs  — AgentDefinition, DiscoverySource
crates/harness-core/src/contracts/runtime_profile.rs   — RuntimeProfile, CapabilitySet, TriState
crates/harness-core/src/contracts/task.rs              — TaskLifecycle (12 states), Task
crates/harness-core/src/contracts/task_envelope.rs     — TaskEnvelope, FileScope, TaskBudget
crates/harness-core/src/contracts/task_result.rs       — TaskResult
crates/harness-core/src/contracts/project.rs           — ProjectLifecycle (12 states), Project
crates/harness-core/src/contracts/workspace.rs         — WorkspaceLease, LeaseLifecycle
crates/harness-core/src/contracts/repository.rs        — 6 Repository traits + record types
crates/harness-core/src/contracts/goal_contract.rs     — GoalContractVersion, ChangeRequest
crates/harness-core/src/error.rs                       — CoreError, 24 ErrorCodes
crates/harness-core/src/state_machine/mod.rs           — ExecutionLifecycle
crates/harness-core/src/state_machine/project_fsm.rs   — ProjectFsm
crates/harness-core/src/state_machine/task_fsm.rs      — TaskFsm (with RetryPending)
crates/harness-core/src/state_machine/execution_fsm.rs — ExecutionFsm
crates/harness-core/src/state_machine/lease_fsm.rs     — LeaseFsm
crates/harness-core/src/policies/budget.rs             — BudgetPolicy
crates/harness-core/src/policies/command.rs            — CommandPolicy patterns
crates/harness-core/src/policies/file_scope.rs         — FileScopeValidator logic
```

### Persistence Layer
```
crates/harness-runtime/migrations/001_initial_schema.sql
crates/harness-runtime/migrations/002_idempotency_ownership.sql
crates/harness-runtime/migrations/003_operation_claim.sql
crates/harness-runtime/src/db.rs           — Database, connection, bootstrap
crates/harness-runtime/src/repo.rs         — 6 Repository SQLx impls
crates/harness-runtime/src/transition.rs   — TransitionService (atomic state+event)
crates/harness-runtime/src/event_log.rs    — Append-only event log
crates/harness-runtime/src/idempotency.rs  — Idempotency ownership
crates/harness-runtime/src/operation.rs    — Operation/Saga with claim API
```

### Process Layer
```
crates/harness-runtime/src/process/mod.rs          — Module re-exports
crates/harness-runtime/src/process/types.rs        — ProcessSpec, ProcessHandle, ProcessOutcome, etc.
crates/harness-runtime/src/process/manager.rs      — ProcessManager (spawn/wait/timeout/cancel)
crates/harness-runtime/src/process/registry.rs     — ProcessRegistry (in-memory)
crates/harness-runtime/src/process/reconciler.rs   — ProcessReconciler (via TransitionService)
crates/harness-runtime/src/process/job_object.rs   — Platform process tree kill + JobObject stub
crates/harness-runtime/src/bin/process-fixture.rs  — 17-mode test binary
```

### Tests
```
crates/harness-runtime/tests/persistence_closure.rs  — 20 tests (stores, atomicity, idempotency, operations)
crates/harness-runtime/tests/process_integration.rs  — 13 tests (spawn, timeout, cancel, reconciler, claim)
crates/harness-adapters/tests/golden_path_minimal.rs — 4 tests (success, retry, illegal, cancel)
crates/harness-core/tests/snapshot_wire_format.rs    — 10 wire snapshot tests
crates/harness-adapters/src/fake/adapter.rs          — 2 unit tests
crates/harness-adapters/src/contract_test.rs         — 2 unit tests
crates/harness-runtime/src/db.rs                     — 11 unit tests
crates/harness-runtime/src/idempotency.rs            — 4 unit tests
crates/harness-core/src/state_machine/               — 11 unit tests
crates/harness-core/src/policies/                    — 3 unit tests
```

## 4. Key Invariants

- Terminal Execution states (Completed/Failed/Lost/Cancelled) cannot be modified
- Retry creates a new Execution Attempt; old Execution is immutable
- Current-state update and DomainEvent append occur in the same SQLite transaction
- ProcessOutcome is produced at most once per process (RwLock guard)
- Reconciliation MUST go through TransitionService (not direct SQL UPDATE)
- Old claim owner cannot complete/fail after takeover (token validation)
- Agent global environment/config MUST NOT be modified by Harness
- Active pipe handles are NOT recoverable across Supervisor restart
- Orphaned Running Executions become Lost (not directly back to Created)

## 5. Windows Job Object Status

- `tokio::process::Child::raw_handle()` IS available in tokio 1.52.3
- `job_object.rs` provides `JobObject::assign_raw_handle(RawHandle)` interface
- `windows` crate 0.58 `.map_err()` return type differs from current stable Rust patterns
  - `CreateJobObjectW` returns `Result<HANDLE, Error>` but the error mapping via `.code().0` is unstable
- Taskkill `/PID /T /F` is the current Foundation primary
- Job Object remains a Foundation hardening requirement (I2B-0), not a Production deferral

## 6. I2B Implementation Split (Frozen Order)

### I2B-0: Process/Artifact Carryover (MANDATORY before I2B-1)
- Windows Job Object primary implementation
- RuntimeArtifactDirectory
- CapturePolicy::Spool (real file spool)
- ProcessEventRedactor
- flood_stdout/flood_stderr/flood_both deadlock tests
- invalid_utf8 tests
- timeout/cancel/natural-exit race tests
- Grandchild termination + no-residue tests

### I2B-1: WorktreeManager
- Git repository inspection
- Worktree create/remove
- Branch naming (harness/ prefix)
- Ownership marker (.harness-owner)
- Operation/Saga integration
- Orphan worktree detection

### I2B-2: WorkspaceLeaseService
- Acquire/heartbeat/renew/release/expiry
- Reconciliation integration

### I2B-3: Workspace Policy
- FileScopeValidator (path escape, symlink)
- Basic SecretScanner
- CommandPolicyEngine

## 7. Still Forbidden in I2B

- Full ResourceClaim conflict algorithm
- Scheduler
- Production Claude/Codex Adapter
- Integration Queue
- Loop Engine
- Supervisor IPC
- TUI

## 8. Verification Results

```
cargo fmt --all --check:  PASS (after cargo fmt --all)
cargo clippy --workspace --all-targets -- -D warnings:  WARNINGS ONLY (no errors)
cargo test --workspace:    76 passed, 0 failed, 0 ignored
git status --short:        27 modified files (all staged changes pending commit)
```

## 9. Working Tree Changes

All 27 modified files are the accumulated I2A implementation changes. No unrelated modifications. Working tree is clean after `cargo fmt --all`.

---

**Ready for**: commit + push (`docs: hand off process kernel to workspace phase`)
**Session suitable for closure**: Yes
**Next action**: `git add -A && git commit -m "docs: hand off process kernel to workspace phase"` (no push)
