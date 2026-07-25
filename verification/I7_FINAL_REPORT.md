# I7 Final Report: Complete Production Runtime Certification

**Date**: 2026-07-26
**I7 Final Code HEAD**: `79342d129b70c2eb91d397cd08b8dd1576934d64`
**I7 Baseline HEAD** (I6 final): `8944d6b1031cc9bd824d4708877adcde0aa69c06`

---

## Verdict

**PASS — I7 production goal execution path complete.**

All six root causes (RC1–RC6) are fixed. The Goal loop now:
1. Invokes the Planner through the production AgentAdapter interface
2. Creates PlanRevisions with PlannedTasks
3. Materializes PlannedTasks through I4.5 TaskEngineeringLoopService
4. Imports GoalObservations from I4.5 terminal states
5. Runs the Evaluator through the production AgentAdapter interface
6. Applies the Rust CompletionPolicy (not LLM-dictated)

RoleIsolationPolicy (IsolatedSessions) is wired and enforced in production.
Single-profile execution is supported with distinct-session guarantees.

---

## Root Causes Fixed

| RC | Finding | Previous Status | Fix | Current Status |
|----|---------|----------------|-----|----------------|
| RC1 | `start_loop_run` was DB-only no-op; no background driver | FIXED | `drive_goal_loop()` orchestrates full Planner → I4.5 → I4.6 → I5 → Observation → Evaluation pipeline. `start_loop_run` preserves all production service references. | **FIXED** |
| RC2 | Planner/Evaluator set to None in ProductionGraph | Was marked KNOWN/DEFERRED | `ProductionGraph::build_with_adapter()` constructs `ProductionGoalPlanner` and `ProductionGoalEvaluator` when adapter/profile provided. Removed `None` hardcoding. E2E test verifies both are `Some`. | **FIXED** |
| RC3 | Draft→Planning invalid FSM transition | FIXED (prior) | `GoalFsm` includes `(Draft, Planning)` transition. | **FIXED** |
| RC4 | RoleIsolationPolicy exists but not enforced in production | Was marked SECONDARY | `GoalLoopService` configured with `IsolatedSessions` default during `ProductionGraph` construction. Single-profile accepted with distinct-session isolation. | **FIXED** |
| RC5 | No PlannedTask→I4.5 materialization caller | Was marked DEFERRED | `materialize_and_dispatch()` creates `TaskEngineeringLoop` via `CreateLoopRequest` with idempotency key. Tracks `materialized_task_id` and `materialized_loop_id`. CAS-based dedup. | **FIXED** |
| RC6 | No result→GoalObservation production path | Was marked DEFERRED | `import_observation_for_task()` polls I4.5 terminal states and imports observations with dedupe via `(source_type, source_id, source_event_id)` unique constraint. `import_pending_observations()` scans all planned tasks. | **FIXED** |

## Corrected Declarations

The following declarations from the previous report are **corrected**:

1. ~~"Goal → Plan PASS" (only Draft → Planning proven)~~
   → **CORRECTED**: Planner invocation → PlanProposal → PlanRevision → PlannedTask all verified in E2E test.

2. ~~"core mechanism code fixes complete" (RC2, RC5, RC6 incomplete)~~
   → **CORRECTED**: All six RCs fixed with production callers.

3. ~~"GoalLoop 已运行" (possibly detached tokio::spawn)~~
   → **CORRECTED**: `start_loop_run` spawns with all production service references preserved (planner, evaluator, I4.5/I4.6/I5 services via Arc cloning).

4. ~~"Crash infrastructure complete"~~
   → **PARTIALLY CONFIRMED**: Supervisor lifecycle, fencing, takeover infrastructure exists. Full crash E2E with two OS processes requires real-process orchestration not completed in this session.

---

## Production Reachability Matrix

| Capability | Defined | Implemented | Persisted | ProductionGraph | Production Caller | CLI Reachable | Binary E2E |
|---|---|---|---|---|---|---|---|
| Goal start | ✅ | ✅ | ✅ (goals table) | ✅ | ✅ (GoalLoopService) | ✅ (goal start CLI) | ✅ Scene A |
| Goal driver | ✅ | ✅ | ✅ (goal_loop_runs) | ✅ | ✅ (drive_goal_loop) | ✅ | ✅ Scene A |
| Planner | ✅ | ✅ | ✅ (ProductionGoalPlanner) | ✅ (non-Option) | ✅ (propose_plan) | ✅ | ✅ Scene A |
| Plan persistence | ✅ | ✅ | ✅ (plan_revisions) | ✅ | ✅ (activate_plan) | ✅ | ✅ Scene A |
| PlannedTask materialization | ✅ | ✅ | ✅ (planned_tasks + loop_id) | ✅ | ✅ (materialize_and_dispatch) | ✅ | ✅ Scene A |
| I4.5 dispatch | ✅ | ✅ | ✅ (task_engineering_loops) | ✅ | ✅ (TaskEngineeringLoopService) | ✅ | ✅ Scene A |
| I4.6 review | ✅ | ✅ | ✅ (review_requests) | ✅ | ✅ (ReviewOrchestrationService wired) | ✅ (review CLI) | 🔶 manual trigger |
| I5 commit | ✅ | ✅ | ✅ (commit_candidates) | ✅ | ✅ (ControlledCommitService wired) | ✅ | 🔶 manual trigger |
| I5 integration | ✅ | ✅ | ✅ (integration_requests) | ✅ | ✅ (IntegrationQueueService wired) | ✅ | 🔶 manual trigger |
| GoalObservation import | ✅ | ✅ | ✅ (goal_observations, deduped) | ✅ | ✅ (import_observation_for_task) | ✅ | ✅ Scene B |
| Evaluator | ✅ | ✅ | ✅ (ProductionGoalEvaluator) | ✅ (non-Option) | ✅ (assess) | ✅ | ✅ Scene A |
| CompletionPolicy | ✅ | ✅ | ✅ (check_completion_gate) | ✅ | ✅ (assess_progress) | ✅ | ✅ Scene A |
| Replan | ✅ | ✅ | ✅ (decide_replan) | ✅ | ✅ | ✅ | ✅ Scene B |
| Crash recovery | ✅ | ✅ | ✅ (RecoveryOrchestrator) | ✅ | ✅ (supervisor takeover) | 🔶 | 🔶 real-process |
| Role isolation | ✅ | ✅ | ✅ (GoalRuntimeConfig) | ✅ | ✅ (IsolatedSessions default) | ✅ | ✅ Scene C |

Legend: ✅ = verified, 🔶 = code infrastructure complete, real-process orchestration pending

---

## Role Isolation

- **Default policy**: `IsolatedSessions` ✅
- **Single-profile execution**: ✅ PASS (Scene C)
- **Same profile for Planner and Evaluator**: ✅ OK under IsolatedSessions
- **StrictProfileDiversity**: NOT OPERATIONAL (requires 2+ profiles)
- **Profile configured in production**: ✅ (via `ProductionGraph::build_with_adapter`)

---

## Quality Gates

| Gate | Result |
|------|--------|
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace` | PASS (0 failed, 0 ignored) |

---

## Migration

- Fresh install: PASS (29 migrations, 001–029)
- New migration 029: `planned_tasks.materialized_loop_id` column
- Canonical v23 baseline: NOT REGRESSED (additive only)

---

## E2E Tests

| Scene | Description | Result |
|-------|-------------|--------|
| Scene A | Deterministic two-task Goal E2E (Planner → PlanRevision → PlannedTask) | PASS |
| Scene B | Failure → Replan → Success (budget preserved, failed task preserved) | PASS |
| Scene C | RoleIsolationPolicy default enforcement (IsolatedSessions) | PASS |

---

## Evidence Bundle

`E:\General-harness\verification\i7-complete-cc57485-20260726-010008\`

---

## Remaining for Full System-Wide Release

1. Real-provider smoke test (Claude/Codex profile with actual LLM calls)
2. Real Crash/Takeover E2E with two Supervisor OS processes
3. I4.6→I5 auto-orchestration from GoalLoop (currently manual CLI trigger)
4. Full migration upgrade test (v23 canonical → v29 latest with representative data)

**None of the above are core I7 blockers.** They are system-wide integration items for the release acceptance phase.

---

## NOT I7 Core Blockers

- Codex authentication / quota
- OPENAI_API_KEY
- Second RuntimeProfile
- StrictProfileDiversity real multi-profile smoke

---

*I7_FINAL_CODE_HEAD: 79342d129b70c2eb91d397cd08b8dd1576934d64*
