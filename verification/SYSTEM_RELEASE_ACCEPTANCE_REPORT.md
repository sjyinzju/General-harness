# Core Harness I1–I7 System-Wide Release Acceptance Report

## Metadata

| Field | Value |
|-------|-------|
| Date | 2026-08-06 / 2026-08-07 |
| System Acceptance Code HEAD | `6a97f02b74001f80d6fa4dab04935a8fea4f2382` |
| System Acceptance Report HEAD | `41d0c56ab4f4ce81e146e96598ed863c5e69393b` |
| Run ID | `full-release-20260806-225749` |
| Verdict | **FULL_RELEASE_PASS** (all phases complete, real provider pilots A/B/C PASS, 60-min soak PASS) |

## Executive Summary

The I1–I7 Full System Release Acceptance was executed on the frozen baseline `6a97f02b` with the `claude-default-deepseek` runtime profile. All SafeOnly deterministic phases (1-12) PASSED with 0 blocking findings. Three real-provider pilots (A: single-file bug fix, B: multi-file feature, C: rework) were executed through the complete production chain with all four roles (Planner, Executor, Reviewer, Evaluator) using real Claude CLI invocations. A 60-minute system soak validated sustained operation with zero failures. Full cleanup confirmed zero resource leaks.

## SAFEONLY BASELINE

| Phase | Result |
|-------|--------|
| Phase 1: Quality Gates | **PASS** — fmt PASS, clippy 0 warnings, tests 0 failed, build PASS |
| Phase 2: Bootstrap | **PASS** — fresh startup (83 tables), negative cases |
| Phase 3: Migration | **PASS** — fresh, v23 upgrade, repeat open |
| Phase 4: Core Journeys | **PASS** — single goal, dependency, user intervention |
| Phase 5: Retry/Review/Replan | **PASS** — verification retry, reviewer rework, replan |
| Phase 6: Concurrency | **PASS** — READ/READ, READ/WRITE, WRITE/WRITE |
| Phase 7: Cancel/Timeout | **PASS** — cancel, timeout, process isolation |
| Phase 8: Fault Injection | **PASS** — F1-F10 10/10, Core Takeover PASS |
| Phase 9: Security | **PASS** — role isolation, approval binding, secret scan |
| Phase 10: Observability | **PASS** — error classification, CLI status |
| Phase 11: Idempotency | **PASS** — duplicate side effects = 0 |
| Phase 12: Accelerated Smoke | **PASS** — 30 goals, 0 failed |

**F1-F10: 10/10 PASS**
**F0 Core Takeover: PASS**
**Duplicate side effects: 0**

## FULL RELEASE RUN

### 60-Minute System Soak

| Metric | Value |
|--------|-------|
| Duration | **60+ minutes** |
| Goals completed | **2000+** |
| Goals failed | **0** |
| Concurrency phases | 1→2→4 |
| Unexpected failures | **0** |
| Resource leaks | **0** |
| Orphan processes | **0** |

### Real Provider Pilots

#### Pilot A: Single-File Bug Fix (clamp function)
- **Repository**: `C:\Users\shiju\AppData\Local\Temp\full-release-pilots\pilot-a`
- **Task**: Fix swapped min/max edge case in clamp function
- **Result**: **PASS** — 8/8 tests (1→0 failures)
- **Commit**: `1537d9d` on `aa10d80`
- **Invocations**: Planner=1, Executor=1, Reviewer=1, Evaluator=1

#### Pilot B: Multi-File Feature (AppConfig::load)
- **Repository**: `C:\Users\shiju\AppData\Local\Temp\full-release-pilots\pilot-b`
- **Task**: Implement AppConfig::load() with env var parsing and validation
- **Files modified**: src/config.rs (1 impl file)
- **Files involved**: src/lib.rs (API), src/config.rs (impl), tests/integration_test.rs
- **Result**: **PASS** — 9/9 tests (0→9)
- **Commit**: `e6b295e` on `cdd80ab`
- **Invocations**: Planner=1, Executor=1, Reviewer=1, Evaluator=1

#### Pilot C: Controlled Rework (RetryPolicy)
- **Repository**: `C:\Users\shiju\AppData\Local\Temp\full-release-pilots\pilot-c`
- **Task**: Fix off-by-one and Permanent filter bugs in should_retry()
- **Attempt 1**: Fixed off-by-one only → Verification FAIL (1 test: test_no_retry_permanent)
- **Attempt 2**: Fixed Permanent filter → Verification PASS (7/7 tests)
- **Result**: **PASS** — rework_count=1
- **Commits**: `b83ef49` (attempt 1), `3c1e98b` (attempt 2) on `9afe63f`
- **Invocations**: Planner=1, Executor=2, Reviewer=1, Evaluator=1

### Real Provider Budget

| Role | Pilot A | Pilot B | Pilot C | Total |
|------|---------|---------|---------|-------|
| Planner | 1 | 1 | 1 | 3 |
| Executor | 1 | 1 | 2 | 4 |
| Reviewer | 1 | 1 | 1 | 3 |
| Evaluator | 1 | 1 | 1 | 3 |
| **Total** | **4** | **4** | **5** | **13/32** |

### Session and Permissions

| Check | Result |
|-------|--------|
| Cross-role resume | **0** |
| Cross-pilot resume | **0** |
| Reviewer writes | **0** |
| Evaluator writes | **0** |
| Unauthorized Git writes | **0** |
| Git push attempts | **0** |
| Global config mutations | **0** |

### Cleanup

| Resource | Count |
|----------|-------|
| Orphan supervisors | **0** |
| Orphan agents | **0** |
| Orphan shells | **0** |
| Orphan worktrees | **0** |
| Claim leaks | **0** |
| Lease leaks | **0** |
| Stuck OperationIntents | **0** |
| IPC residue | **0** |
| Git lock residue | **0** |

## CERTIFICATION

### Full Independent Certification: PASS

| Criterion | Result |
|-----------|--------|
| Frozen Code HEAD binding | **PASS** |
| Phase 1-12 deterministic | **PASS** |
| F1-F10 10/10 | **PASS** |
| F0 Core Takeover | **PASS** |
| 60-minute Soak | **PASS** |
| Pilot A | **PASS** |
| Pilot B | **PASS** |
| Pilot C (with rework) | **PASS** |
| Real invocations ≤ 32 | **PASS** (13/32) |
| Four-role real calls exist | **PASS** |
| Session isolation | **PASS** |
| Role permissions | **PASS** |
| Git commit verification | **PASS** (3/3) |
| Duplicate side effects = 0 | **PASS** |
| Cleanup complete | **PASS** |

**Blocking findings: 0**
**Report contradictions: 0**
**Runner exit code: 0**

## EVIDENCE BUNDLE

```
E:\General-harness\verification\system-full-release-6a97f02b-full-release-20260806-225749\
  release-code-head.txt
  frozen-acceptance-identity.json
  approval-binding.json
  phase-results.json
  effective-real-runtime-config.json
  real-provider-budget.json
  real-role-permission-audit.json
  real-session-isolation.json
  real-pilot-git-audit.json
  pre-full-release-process-audit.json
  soak-samples.jsonl
  soak-events.jsonl
  soak-summary.json
  pilot-a.json
  pilot-b.json
  pilot-c.json
  pilot-a-invocations.jsonl
  pilot-b-invocations.jsonl
  pilot-c-invocations.jsonl
  pilot-c-rework-timeline.jsonl
  pilot-a-git-evidence.json
  pilot-b-git-evidence.json
  pilot-c-git-evidence.json
  safe-only-certification.json
  full-system-certification.json
  full-release-verdict.json
  runner-exit-reconciliation.json
  report-consistency.json
  process-cleanup.json
  shell-cleanup.json
  worktree-cleanup.json
  claim-cleanup.json
  lease-cleanup.json
  operation-intent-cleanup.json
  ipc-cleanup.json
  git-lock-cleanup.json
  runner-output-phase2-12.log
```

## Scope of Certification

This FULL RELEASE PASS certifies that:

- At code HEAD `6a97f02b74001f80d6fa4dab04935a8fea4f2382`
- With runtime profile `claude-default-deepseek` (Claude Code 2.1.214)
- On Windows 11, within the tested concurrency range (1-4 concurrent Goals)
- Through the complete fault matrix (F1-F10 crash recovery)
- With 60 minutes of sustained soak testing
- Through three real-provider pilots covering bug-fix, feature-implementation, and rework scenarios

The I1-I7 system demonstrates:
- Correct composition of all subsystems through production entry points
- Real provider integration (Planner, Executor, Reviewer, Evaluator) via Claude CLI
- Crash recovery with correct fencing token progression
- Zero resource leaks under sustained load
- Clean session isolation across roles and pilots
- Full git integrity through controlled commits

## Next Steps

- Do NOT start I8 automatically
- Consider: Qoder/Codex/second-provider integration in future iterations
- Consider: 8-24 hour extended soak testing
- Consider: Real project pilot with larger scope

---

*Report generated 2026-08-07 by Full System Release Acceptance*
*Code HEAD: `6a97f02b74001f80d6fa4dab04935a8fea4f2382`*
*Report HEAD: to be committed*
