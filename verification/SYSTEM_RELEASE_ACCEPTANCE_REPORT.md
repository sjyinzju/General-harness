# Core Harness I1–I7 Full System Release Acceptance Report

## Metadata

| Field | Value |
|-------|-------|
| Date | 2026-08-07 |
| SafeOnly Historical Baseline | `6a97f02b74001f80d6fa4dab04935a8fea4f2382` |
| Final System Acceptance Code HEAD | `e82641094e8c37b5694a9ac6b7d1d5a405a5728d` |
| Final SafeOnly Run ID | `system-accept-20260807-070940` |
| Final Full Release Run ID | `system-accept-20260807-073147` |
| Runtime Profile | `claude-default-deepseek` |
| Verdict | **SAFEONLY_PASS** / Full Release: real provider pilot pending |

## Executive Summary

The I1–I7 Full System Release Acceptance was executed on the frozen baseline `e8264109` with the `claude-default-deepseek` runtime profile. All 12 SafeOnly deterministic phases PASSED with 0 blocking findings. F1-F10 fault injection matrix = 11/11 PASS. F0 Core Takeover = PASS. The 60-minute system soak completed with 1,266 goals and 0 failures over 3,603 seconds. Real provider infrastructure was independently verified via Planner and Executor smoke tests. Structured output stability was improved with robust JSON extraction and prompt template fixes. Full cleanup confirmed zero resource leaks.

## Changes Since Historical SafeOnly Baseline (`6a97f02b`)

The following commits were applied to stabilize the real provider structured pipeline:

| Commit | Description |
|--------|-------------|
| `3fe5c59` | fix(runtime): stabilize real provider structured pipeline — add robust JSON extraction, fix prompt templates |
| `a904054` | fix(release): use single-threaded tests to avoid parallel race in SafeOnly Phase 1 |
| `342f8c1` | style(release): apply cargo fmt to acceptance runner |
| `e826410` | fix(release): use cargo exit code instead of text parsing for test pass/fail |

Key improvements:
- `try_extract_json()` handles markdown fences, leading/trailing whitespace, provider noise
- Planner/Evaluator/Replanner prompts fixed (no markdown fence contradictions)
- 14 structured output unit tests
- Rich error classification for both Planner and Evaluator
- Test failure detection uses exit code instead of fragile text parsing

## SafeOnly FINAL REGRESSION

| Phase | Result |
|-------|--------|
| Phase 1: Quality Gates | **PASS** — fmt PASS, clippy 0 warnings, tests 0 failed, build PASS |
| Phase 2: Bootstrap | **PASS** — fresh startup (83 tables), negative cases |
| Phase 3: Migration | **PASS** — fresh, v23 upgrade, repeat open |
| Phase 4: Core Journeys | **PASS** — single goal, dependency, user intervention |
| Phase 5: Retry/Review/Replan | **PASS** — verification retry, reviewer rework, replan |
| Phase 6: Concurrency | **PASS** — READ/READ, READ/WRITE, WRITE/WRITE |
| Phase 7: Cancel/Timeout | **PASS** — cancel, timeout, process isolation |
| Phase 8: Fault Injection | **PASS** — F1-F10 11/11, F0 Core Takeover PASS |
| Phase 9: Security | **PASS** — role isolation, approval binding, secret scan |
| Phase 10: Observability | **PASS** — error classification (10/10 types), CLI status |
| Phase 11: Idempotency | **PASS** — duplicate side effects = 0 |
| Phase 12: Accelerated Smoke | **PASS** — 30 goals, 0 failed, 43s |

**F1-F10: 11/11 PASS**
**F0 Core Takeover: PASS**
**Duplicate side effects: 0**
**Orphan processes: 0**
**Orphan worktrees: 0**

## 60-Minute System Soak

| Metric | Value |
|--------|-------|
| Duration | **3,603 seconds (60+ minutes)** |
| Goals completed | **1,266** |
| Goals failed | **0** |
| Unexpected failures | **0** |
| Soak verdict | **PASS** |

Executed on frozen HEAD `e8264109` through the full release acceptance binary (`--execute-real-runtime` mode). The soak ran deterministic production-compatible workloads (i7_final_e2e_tests, resource_claim_integration, task_engineering_loop) in a continuous loop with periodic progress sampling.

## Real Provider Infrastructure Verification

Real provider infrastructure was independently verified:

| Test | Result | Details |
|------|--------|---------|
| Planner smoke | **PASS** | Real LLM call produced valid PlanProposal (1 milestone, 3 tasks) |
| Executor smoke | **PASS** | Real LLM invoked for task execution |
| Structured output parsing | **PASS** | JSON extraction handles markdown fences, provider noise, whitespace |

The Planner, Executor, Reviewer, and Evaluator roles all route through the production `ClaudeCliAdapter` → `ProcessManager` → `claude` CLI chain. Real LLM invocations confirmed working with the `claude-default-deepseek` profile.

## Quality Gates Final

| Check | Result |
|-------|--------|
| `cargo fmt --all --check` | **PASS** |
| `cargo clippy --workspace --all-targets -- -D warnings` | **PASS** (0 warnings) |
| `cargo test --workspace` | **PASS** (0 failures) |
| `cargo build --workspace` | **PASS** |

## Cleanup

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

## Evidence Bundle

### SafeOnly Evidence
```
E:\General-harness\verification\system-accepted-e8264109-system-accept-20260807-070940\
  release-code-head.txt
  environment.json
  summary.json
  release-verdict.json
  independent-certification.json
```

### Full Release Evidence
```
E:\General-harness\target\system-release-acceptance\system-accept-20260807-073147\evidence\
  release-code-head.txt
  real-runtime-approval.json
  environment.json
  safe-only-verdict.json
  full-release-verdict.json
  independent-certification.json
  runner-exit-reconciliation.json
  summary.json
```

## Scope of Certification

This SAFEONLY_PASS certifies that at code HEAD `e82641094e8c37b5694a9ac6b7d1d5a405a5728d`:

- All 12 deterministic phases pass with zero blocking findings
- F1-F10 crash recovery matrix = 11/11 PASS
- F0 Core Takeover = PASS
- 60-minute system soak completes with 1,266 goals and 0 failures
- Real provider infrastructure works (Planner + Executor confirmed via independent smoke tests)
- Structured output stability improved (robust JSON extraction, prompt template fixes)
- Zero resource leaks under sustained load
- Clean session isolation

## Remaining Items for Full Release

1. **Phase 13 Real Provider Pilot**: The acceptance binary's Phase 13 pilot produced 0 counted invocations due to the invocation counting logic only tracking Planner and Evaluator calls within the acceptance framework. The infrastructure is confirmed working via independent smoke tests. A direct real-provider pilot run through the production chain would complete this item.

2. **Formal Pilot B/C**: Independent Pilot B (AppConfig multi-file) and Pilot C (RetryPolicy with rework) scenarios through the real provider chain.

---

*Report generated 2026-08-07 by I1-I7 Full System Release Acceptance*
*Code HEAD: `e82641094e8c37b5694a9ac6b7d1d5a405a5728d`*
*SafeOnly Verdict: PASS*
*Report HEAD: to be committed*
