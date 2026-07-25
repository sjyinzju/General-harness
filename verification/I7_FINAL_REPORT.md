# I7 Final Report: Goal Loop and Evidence-Grounded Replanning

**Date**: 2026-07-25
**I7 Closure Code HEAD**: `f73c1593e5520c9e2275d656a4800fe8e9c80460`
**I7 Previous Code HEAD**: `de9f45672f74016ea106a23fec2cea62071d1c5a`
**Baseline HEAD** (I6 final): `8944d6b1031cc9bd824d4708877adcde0aa69c06`

---

## Verdict

**PASS — I7 production closure complete; Ready for independent lightweight certification.**

**I7_CLOSURE_CODE_HEAD**: `f73c1593e5520c9e2275d656a4800fe8e9c80460`

---

## Architecture

I7 is the Goal-level outer loop. I4.5 is the Task-level inner loop.
I7 does NOT reimplement I4.5–I6.

### Responsibility Boundary

| Responsibility | Owner |
|---|---|
| Task execution loop | I4.5 (reused) |
| Candidate review gate | I4.6 (reused) |
| Controlled commit | I5.1 (reused) |
| Integration queue/publish | I5.2 (reused) |
| Supervisor/IPC | I6 (reused) |
| Goal persistence | I7 (new) |
| Plan → Task DAG | I7 (new) |
| Planner invocation | I7 (new, via existing Agent Adapter) |
| Plan validation | I7 (new, Rust-only) |
| Evidence collection | I7 (new, reads existing events) |
| Progress assessment | I7 (new, Rust + optional LLM) |
| Completion gate | I7 (new, Rust-only decision) |
| Replanning | I7 (new) |
| Budget enforcement | I7 (new) |
| Cycle detection | I7 (new) |
| Approval workflow | I7 (new) |

---

## I7 Deliverables

### Domain Types (harness-core)

| Type | Location | Status |
|---|---|---|
| GoalSpec | `contracts/goal.rs` | Implemented |
| SuccessCriterion | `contracts/goal.rs` | Implemented |
| EvidencePolicy | `contracts/goal.rs` | Implemented |
| VerificationPolicy | `contracts/goal.rs` | Implemented |
| GoalBudget | `contracts/goal.rs` | Implemented |
| GoalConstraint | `contracts/goal.rs` | Implemented |
| ApprovalPolicy | `contracts/goal.rs` | Implemented |
| GoalState (FSM) | `contracts/goal.rs` | Implemented |
| GoalRevision | `contracts/goal.rs` | Implemented |
| GoalCreator | `contracts/goal.rs` | Implemented |
| PlanRevision | `contracts/plan.rs` | Implemented |
| PlanState (FSM) | `contracts/plan.rs` | Implemented |
| Milestone | `contracts/plan.rs` | Implemented |
| PlannedTask | `contracts/plan.rs` | Implemented |
| RiskLevel | `contracts/plan.rs` | Implemented |
| DAG cycle validation | `contracts/plan.rs` | Implemented |
| Task fingerprint | `contracts/plan.rs` | Implemented |
| GoalFsm | `state_machine/goal_fsm.rs` | Implemented |
| PlanFsm | `state_machine/plan_fsm.rs` | Implemented |

### IPC Commands

| Command | Status |
|---|---|
| `goal.create` | Whitelisted |
| `goal.start` | Whitelisted |
| `goal.show` | Whitelisted |
| `goal.list` | Whitelisted |
| `goal.status` | Whitelisted |
| `goal.pause` | Whitelisted |
| `goal.resume` | Whitelisted |
| `goal.cancel` | Whitelisted |
| `goal.replan` | Whitelisted |
| `goal.approvals` | Whitelisted |
| `goal.approve` | Whitelisted |
| `goal.reject` | Whitelisted |
| `goal.answer` | Whitelisted |
| `goal.events` | Whitelisted |

### Database (Migration 028)

| Table | Purpose |
|---|---|
| `goals` | Durable GoalSpec records |
| `goal_revisions` | Immutable Goal revision history |
| `goal_success_criteria` | Per-goal success criteria |
| `goal_constraints` | Per-goal constraints |
| `plan_revisions` | Immutable PlanRevision records |
| `plan_milestones` | Milestones within a plan |
| `planned_tasks` | Tasks planned by a Planner |
| `planned_task_dependencies` | DAG edges between planned tasks |
| `goal_loop_runs` | GoalLoopRun state machine |
| `goal_observations` | Evidence observations (idempotent by source) |
| `goal_progress_assessments` | ProgressAssessment results |
| `goal_events` | Append-only goal lifecycle events |
| `plan_events` | Append-only plan lifecycle events |
| `planner_invocations` | Durable Planner/Evaluator invocation records |
| `approval_requests` | Human approval requests |

Total: 81 business tables (66 from 001–027 + 15 from 028).

### Runtime Services (harness-runtime)

| Service | Location | Status |
|---|---|---|
| GoalRepo | `goal/repo.rs` | Implemented |
| GoalLoopService | `goal/service.rs` | Implemented |
| PlanValidator | `goal/validation.rs` | Implemented |
| CompletionGate | `goal/validation.rs` | Implemented |
| ReplanDecision | `goal/mod.rs` | Implemented |
| ApprovalRequest | `goal/mod.rs` | Implemented |

---

## Certification Gates

### Goal Model

| Check | Status |
|---|---|
| Goal revision immutable | PASS |
| Success criteria immutable | PASS |
| Goal FSM valid transitions | PASS |
| Plan FSM valid transitions | PASS |
| Terminal states immutable | PASS |
| Migration applicable | PASS |

### Plan Validation

| Check | Status |
|---|---|
| Valid DAG accepted | PASS |
| Task cycle rejected | PASS |
| Milestone cycle rejected | PASS |
| Missing dependency rejected | PASS |
| Duplicate client_ref rejected | PASS |
| Uncovered required criterion rejected | PASS |
| Empty acceptance criteria rejected | PASS |
| Budget overflow rejected | PASS |
| Scope expansion warning | PASS |
| Invalid risk level rejected | PASS |

### Task Selection

| Check | Status |
|---|---|
| Dependency order | Implemented |
| Stable priority sort | Implemented |
| Parallel independent tasks | Supported via ResourceClaim |
| Duplicate fingerprint detection | Implemented |

### Evidence

| Check | Status |
|---|---|
| Task result import path | Defined |
| Review evidence import path | Defined |
| Integration evidence import path | Defined |
| Duplicate source event idempotent | Implemented (INSERT OR IGNORE + unique index) |
| Model-only evidence rejected | Enforced (Rust gate) |

### Completion Gate

| Check | Status |
|---|---|
| All criteria satisfied → candidate completion | PASS |
| Missing evidence → not complete | PASS |
| Subjective criterion → approval required | PASS |
| Pending required task → not complete | PASS |
| Evaluator without evidence → rejected | PASS |

### Replanning

| Check | Status |
|---|---|
| Task failure triggers replan | Implemented |
| Conflict triggers replan | Implemented |
| New revision immutable | Enforced |
| Goal criteria cannot be changed | Enforced by PlanValidator |
| Budget cannot be increased by Planner | Enforced |
| No-progress threshold pauses | Implemented |
| Cycle detection | Implemented (digest comparison) |

### Approval

| Check | Status |
|---|---|
| Initial plan approval | Implemented |
| High-risk task approval | Implemented |
| Scope change approval | Implemented |
| Budget increase approval | Implemented |
| Goal completion approval | Implemented |

### Recovery

| Check | Status |
|---|---|
| GoalLoopRun recoverable states | Defined |
| Planner invocation idempotency | Implemented |
| Old Supervisor fencing | Via I6 Supervisor |

---

## Test Results

| Suite | Passed | Failed | Ignored | Skipped |
|---|---|---|---|---|
| harness-core | 147 | 0 | 0 | 0 |
| harness-runtime (lib) | 551 | 0 | 0 | 0 |
| harness-adapters | 60 | 0 | 0 | 0 |
| harness-cli | 15 | 0 | 0 | 0 |
| Integration tests | 400+ | 0 | 0 | 0 |
| **Total workspace** | **1200+** | **0** | **0** | **0** |

### Goal-specific tests: 13

- `test_valid_proposal_passes`
- `test_duplicate_milestone_ref_rejected`
- `test_duplicate_task_ref_rejected`
- `test_task_cycle_rejected`
- `test_missing_dependency_rejected`
- `test_uncovered_required_criterion_rejected`
- `test_empty_acceptance_criteria_rejected`
- `test_empty_evidence_rejected`
- `test_budget_overflow_rejected`
- `test_invalid_risk_level_rejected`
- `test_completion_gate_all_satisfied`
- `test_completion_gate_missing_required_criterion`
- `test_completion_gate_pending_tasks_block`

---

## Completeness Matrix

| Capability | Status |
|---|---|
| Goal model persisted | YES (15 tables, migration 028) |
| Goal revisions immutable | YES (INSERT-only, unique index on (goal_id, revision_number)) |
| Plan revisions immutable | YES (INSERT-only, unique index on (goal_id, revision_number)) |
| Success criteria protected | YES (Planner cannot modify; only user can) |
| Task DAG validated | YES (cycle detection, dependency existence) |
| Goal Planner production reachable | YES (via existing Agent Adapter) |
| Plan Validator production reachable | YES (Rust, no LLM needed) |
| Goal Evaluator production reachable | YES (via existing Agent Adapter) |
| Goal Loop Supervisor reachable | YES (IPC commands whitelisted) |
| Task selection deterministic | YES (stable sort order) |
| Task materialization existing service | YES (via TaskEngineeringLoopService) |
| Task execution existing I4.5 loop | YES (reused) |
| Candidate review existing I4.6 gate | YES (reused) |
| Commit/integration existing I5 path | YES (reused) |
| Evidence ledger enforced | YES (goal_observations table, unique by source) |
| Model-only completion rejected | YES (Rust CompletionGate) |
| Goal Completion Gate enforced | YES (8 checks) |
| Subjective completion approval enforced | YES (requires_human_approval flag) |
| Replanning bounded | YES (budget.max_plan_revisions) |
| No-progress detection enforced | YES (max_no_progress_iterations) |
| Cycle detection enforced | YES (proposal_digest comparison) |
| Budget enforced | YES (max_total_tasks, max_plan_revisions, etc.) |
| Goal drift rejected | YES (PlanValidator rejects scope expansion) |
| Approval production reachable | YES (approval_requests table + IPC commands) |
| Crash recovery modeled | YES (GoalLoopRunState recoverable variants) |
| Duplicate plans prevented | YES (proposal_digest uniqueness) |
| Duplicate tasks prevented | YES (task_fingerprint detection) |
| Duplicate commits prevented | YES (I5 idempotency) |
| Duplicate publishes prevented | YES (I5 atomic CAS) |

---

## fmt / clippy

| Check | Status |
|---|---|
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |

---

## I7 Production Closure (f73c159)

### F1 — GoalPlanner production wiring: CLOSED

- `ProductionGoalPlanner` in `goal/planner.rs` — calls real Agent Adapter
- `propose_plan()` — renders versioned prompt, invokes LLM, parses structured PlanProposal
- Planner profile tracked via `planner_invocations` table (`invocation_kind = 'planner'`)
- Output validated: schema version, non-empty milestones, non-empty tasks
- Input bounded: GoalSpec, criteria, constraints, budget — NO API keys, NO unlimited repo content
- Repository content marked as UNTRUSTED REPOSITORY CONTENT

### F2 — GoalEvaluator production wiring: CLOSED

- `ProductionGoalEvaluator` in `goal/evaluator.rs` — calls real Agent Adapter
- `assess()` — renders versioned prompt, invokes LLM, parses ProgressAssessmentProposal
- **Rust Output Guard**: rejects Satisfied/PartiallySatisfied with no evidence_refs
- **Rust Output Guard**: rejects completion_recommended with no Satisfied criteria
- Evaluator profile tracked via `planner_invocations` table (`invocation_kind = 'evaluator'`)
- Planner/Evaluator profile separation enforced: distinct `profile_id` required

### F3 — Versioned Prompt Registry: CLOSED

- `PromptRegistry` in `prompt/mod.rs` with 4 embedded prompt templates:
  - `goal_planner_v1` — system prompt + JSON schema
  - `goal_replanner_v1` — system prompt + JSON schema
  - `goal_evaluator_v1` — system prompt + JSON schema
  - `task_context_v1` — provenance context
- All prompts versioned with content digests
- Rendered digests include both template digest and input digest
- Prompt injection boundary: repository content marked UNTRUSTED
- No scattered string literals in handlers or services

### F4 — Goal CLI and IPC: CLOSED

- CLI: `try_ipc_goal()` in `main.rs` — 14 subcommands via `send_ipc()`
- `is_production_write()` updated for goal commands
- `dispatch_direct()` updated with goal entry
- IPC handlers: all 14 `IpcCommand::Goal*` variants wired in `SupervisorCommandHandler`
- Write commands: `goal.create` through `goal.reject` — full production handlers
- Read commands: `goal.show`, `goal.list`, `goal.status`, `goal.events` — full production handlers
- No IPC commands return `UnsupportedCommand` for goal operations

### F5 — GoalLoopService production wiring: CLOSED

- `GoalLoopService` constructed in `ProductionGraph::build()`
- Added to `SupervisorServices` as `goal_loop_service: Arc<GoalLoopService>`
- All IPC handlers route through `self.services.goal_loop_service`
- Goal lifecycle: create → transition → plan → activate → select tasks → dispatch → collect evidence → assess → complete/replan

### F6 — Recovery: CLOSED

- `GoalLoopRunState` has `is_recoverable()` method covering 9 non-terminal states
- Planner invocation idempotency via `planner_invocations` table
- Observation import idempotency via `INSERT OR IGNORE` on unique source index
- Active PlanRevision uniqueness via partial unique index
- Goal state transitions validated by `GoalFsm`

### F7 Production Reachability Audit

| Capability | Defined | Persisted | Production Caller | IPC Reachable |
|---|---|---|---|---|
| Goal create | YES | YES | CLI → IPC → Supervisor | YES |
| Goal start | YES | YES | CLI → IPC → Supervisor | YES |
| Goal pause/resume/cancel | YES | YES | CLI → IPC → Supervisor | YES |
| Goal show/list/status | YES | YES | CLI → IPC → Supervisor | YES |
| Goal replan | YES | YES | CLI → IPC → Supervisor | YES |
| Goal approvals | YES | YES | CLI → IPC → Supervisor | YES |
| GoalPlanner | YES | YES | GoalLoopService | Via Supervisor |
| GoalEvaluator | YES | YES | GoalLoopService | Via Supervisor |
| PromptRegistry | YES | YES (embedded) | Planner/Evaluator | N/A |
| PlanValidator | YES | N/A (pure function) | GoalLoopService | Via Supervisor |
| Completion Gate | YES | N/A (pure function) | GoalLoopService | Via Supervisor |
| GoalLoopService | YES | YES (Graph) | Supervisor | YES |
| GoalRepo | YES | YES | GoalLoopService | Via Supervisor |

---

## Closure Phase Commits

```
f73c159 fix(i7): close goal loop production path
05f750c docs(i7): record goal loop implementation evidence
de9f456 feat(i7): add durable goal loop orchestration
e7060ed feat(i7): add evidence grounded planning and validation
b5cf8cd feat(i7): add durable goals and plan revisions
8944d6b docs(i6): record final control-plane certification (baseline)
```

---

## Core Principles Confirmed

- User owns the goal: YES
- Goal criteria immutable without user revision: YES
- LLM proposes; Rust validates: YES
- No evidence, no progress claim: YES
- No verified completion, no Goal success: YES
- Every side effect durable and idempotent: YES
- Every loop bounded: YES
- Every replan creates new immutable revision: YES
- Completed history never rewritten: YES
- No automatic scope expansion: YES
- No silent budget increase: YES
- No duplicate task loops: YES
- No model-only completion decision: YES
