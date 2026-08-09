# Core Harness I1–I7 System-Wide Release Acceptance Plan

## Metadata

- **Plan created**: 2026-08-04
- **Code HEAD at plan creation**: 729b994 (post quality-gate fixes)
- **I7 Acceptance Code HEAD**: 0094034afe00abd79ceedfbccac7143041fcebdb
- **I7 Acceptance Report HEAD**: 59f8e5ac3059f7fa311830426bc19fec20fffe5b
- **I7 Evidence**: Historical raw acceptance evidence pruned after final certification. Compact evidence retained under `verification/delta-certification/`. I7 Acceptance Code HEAD: `0094034afe00abd79ceedfbccac7143041fcebdb`.

## Scope

This plan covers system-wide release acceptance for Core Harness I1–I7. The goal is to verify that all subsystems (I1 Persistence through I7 Goal/Replanning) compose correctly through production entry points (CLI → IPC → Supervisor), handle concurrency, recover from failures, enforce security boundaries, and produce diagnostic evidence.

### Explicitly IN scope
- I1–I7 composition verification through production entry points
- Multi-Goal concurrency and ResourceClaim safety
- Failure injection and crash recovery (real OS Supervisor processes)
- Security boundary enforcement (Planner/Reviewer/Evaluator read-only, Executor scope)
- Idempotency and duplicate side-effect audit
- Diagnostic quality and observability
- Soak test (resource leak detection)
- Representative real-provider pilot (claude-default-deepseek)
- Independent certification
- Evidence bundle generation

### Explicitly OUT of scope
- I8 development
- Long-term memory / experience routing / self-evolution
- Qoder integration
- New Provider integration
- Product UI changes
- Codex or second Provider
- OPENAI_API_KEY configuration

## Architecture

The system acceptance runner (`system-release-acceptance`) is a new binary in `harness-cli` that:

1. Drives the system ONLY through production entry points (CLI, IPC, Supervisor)
2. Reads database/state ONLY for forensic evidence (never writes via SQL)
3. Creates isolated test environments (fresh state dir, SQLite, worktree root, IPC namespace)
4. Executes all 15 acceptance phases
5. Generates structured evidence bundles

## Phases

| Phase | Name | Description |
|-------|------|-------------|
| 1 | Build and Quality Gates | fmt, clippy, test, build |
| 2 | Bootstrap and Installation | Fresh startup, error diagnostics |
| 3 | Migration and Persistent State | 0→latest, v23→latest, data integrity |
| 4 | Core User Journeys | Single Goal, dependency Goal, user intervention |
| 5 | Failure/Retry/Review/Replan | Verification retry, Reviewer rework, Goal replan, budget exhaustion |
| 6 | Multi-Goal Concurrency | READ/READ, READ/WRITE, WRITE/WRITE, Integration queue, fairness |
| 7 | Cancellation/Timeout/Isolation | Goal cancel, Agent timeout, process tree termination, fault isolation |
| 8 | Fault Injection and Crash Recovery | 10 failpoints, Supervisor takeover, fencing |
| 9 | Security/Approval/Permissions | Role isolation, approval binding, secret scan |
| 10 | Observability and Diagnostics | State visibility, error classification |
| 11 | Idempotency and Duplicate Audit | Duplicate side-effect detection |
| 12 | Accelerated Soak | 30 Goals, 60+ min, resource leak detection |
| 13 | Real Provider Pilot | 3 representative Goals via claude-default-deepseek |
| 14 | Independent Certification | Read-only evidence verification |
| 15 | Evidence and Release Verdict | Bundle generation, final verdict |

## Approval Binding

Real-provider phases (Phase 13) require explicit approval binding to:
- Code HEAD
- Run ID
- Isolated writable root
- Profile (claude-default-deepseek)
- Roles (planner, executor, reviewer, evaluator)
- Maximum 32 LLM invocations
- Maximum 2 hours duration
