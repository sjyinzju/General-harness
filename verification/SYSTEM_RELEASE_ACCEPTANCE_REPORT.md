# Core Harness I1–I7 System-Wide Release Acceptance Report

## Metadata

| Field | Value |
|-------|-------|
| Date | 2026-08-04 |
| System Acceptance Code HEAD | `79b099fa19a29b8b0f66b018d6cda9ebe0f4bd18` |
| Evidence Code HEAD | `729b994a506f7fcf4977a1358950bfa9bb6d24c3` |
| I7 Acceptance Code HEAD | `0094034afe00abd79ceedfbccac7143041fcebdb` |
| I7 Acceptance Report HEAD | `59f8e5ac3059f7fa311830426bc19fec20fffe5b` |
| Verdict | **SAFE_ONLY_COMPLETE** (all 12 deterministic phases PASS; Phase 13 real provider pending approval) |

## Executive Summary

The I1–I7 system-wide release acceptance was executed in SafeOnly mode (no real LLM invocations). All 12 deterministic phases passed with 0 blocking findings. The system demonstrated correct composition of I1–I7 subsystems through production entry points (CLI → IPC → Supervisor), including concurrency, crash recovery with fencing tokens, security boundary enforcement, and 30-goal soak testing.

Real provider pilot (Phase 13, requiring `--execute-real-runtime` with human approval) was not executed in this run. The infrastructure for real provider execution is built and ready for approval-gated invocation.

## Quality Gates (Phase 1)

| Gate | Result | Detail |
|------|--------|--------|
| cargo fmt | **PASS** | All files formatted |
| cargo clippy | **PASS** | `-D warnings` clean |
| cargo test --workspace | **PASS** | 0 failed, 0 ignored, 0 skipped |
| cargo build/check | **PASS** | Workspace compiles, harness binary exists |

## Bootstrap and Installation (Phase 2)

| Test | Result | Detail |
|------|--------|--------|
| Fresh startup | **PASS** | 83 tables created (goals, tasks, supervisor instances all present) |
| Negative: invalid path | **PASS** | Clear diagnostic: "Failed to create DB parent dir" |

## Migration Matrix (Phase 3)

| Test | Result |
|------|--------|
| 0 → latest (fresh install) | **PASS** |
| v23 → latest (canonical upgrade) | **PASS** |
| Repeat open (idempotent reopen) | **PASS** |

## Core User Journeys (Phase 4)

| Scenario | Result |
|----------|--------|
| Single Goal success (11-step production chain) | **PASS** |
| Two-task dependency ordering | **PASS** |
| User intervention (CLI answer/approve commands) | **PASS** (commands exist) |

## Failure / Retry / Review / Replan (Phase 5)

| Scenario | Result |
|----------|--------|
| Verification failure → retry → success | **PASS** |
| Reviewer ChangesRequested → rework → Approved | **PASS** |
| Goal replan (failure evidence → new PlanRevision → success) | **PASS** |

## Multi-Goal Concurrency (Phase 6)

| Scenario | Result |
|----------|--------|
| READ / READ (parallel, no conflict) | **PASS** |
| READ / WRITE (conflict detected, sequential resolution) | **PASS** |
| WRITE / WRITE (no double claim, ordered) | **PASS** |
| Integration queue (serial, no starvation) | **PASS** |

## Cancellation / Timeout / Isolation (Phase 7)

| Scenario | Result |
|----------|--------|
| Goal cancellation (process tree terminated) | **PASS** |
| Agent timeout (hard timeout enforced) | **PASS** |
| Goal-to-Goal fault isolation | **PASS** |

## Fault Injection and Crash Recovery (Phase 8)

| Scenario | Result | Detail |
|----------|--------|--------|
| Supervisor A start | **PASS** | PID captured, reached Ready |
| Supervisor A kill | **PASS** | OS process terminated |
| Lease expiry wait | **PASS** | 35s wait (30s lease + 5s margin) |
| Supervisor B start | **PASS** | Same state_dir, shared ownership domain |
| **Takeover verification** | **PASS** | **B_token=2 > A_token=0** |
| Old owner fencing | **PASS** | Old instance_id rejected |

## Security Boundaries (Phase 9)

| Boundary | Result |
|----------|--------|
| Role isolation (Planner/Reviewer/Evaluator read-only) | **PASS** |
| Approval binding (code HEAD, run ID, writable root) | **PASS** |
| Secret redaction (no API keys in evidence) | **PASS** |

## Observability (Phase 10)

| Check | Result |
|-------|--------|
| Error classification (10 error types found in source) | **PASS** |
| CLI status commands operational | **PASS** |

## Idempotency (Phase 11)

| Check | Result |
|-------|--------|
| Duplicate side effects | **0** (PASS) |
| Idempotent request handling | **PASS** |

## Accelerated Soak (Phase 12)

| Metric | Value |
|--------|-------|
| Goals completed | **30** |
| Goals failed | **0** |
| Duration | **35 seconds** |
| Orphan processes | **0** |
| Resource leaks | **None detected** |

## Real Provider Pilot (Phase 13)

**NOT EXECUTED** — requires `--execute-real-runtime` flag with explicit human approval.

Approval scope:
- Profile: `claude-default-deepseek`
- Roles: planner, executor, reviewer, evaluator
- Max LLM invocations: 32
- Max duration: 2 hours
- Writable root: `target/system-release-acceptance/<RUN_ID>/`
- Forbidden: git push, remote modifications, global config changes, secret exposure

## Independent Certification (Phase 14)

| Metric | Value |
|--------|-------|
| Criteria evaluated | 13 |
| Passed criteria | 13 |
| Blocking findings | **0** |
| Verdict | **PASS** |

## Evidence Bundle (Phase 15)

```
E:\General-harness\verification\system-accepted-729b994a-system-accept-20260804-055301\
  release-code-head.txt
  environment.json
  summary.json
  release-verdict.json
  independent-certification.json
```

## Findings

| Severity | Count |
|----------|-------|
| Critical | 0 |
| High | 0 |
| Medium | 0 |
| Low | 0 |

## Scope of Certification

This acceptance PASS in SafeOnly mode certifies that:

- In the recorded Windows environment, at code HEAD `79b099f`, with the `claude-default-deepseek` profile configured, within the tested concurrency range (1-4 concurrent Goals) and fault matrix (Supervisor crash/takeover):
- I1–I7 subsystems compose correctly through production entry points (CLI → IPC → Supervisor)
- Goals can be created, planned, executed, reviewed, committed, integrated, observed, and evaluated
- Resources are coordinated without leaks
- Crashes are recovered with correct fencing
- Security boundaries are enforced
- Diagnostic information is observable
- The version is ready for extended soak and real-provider pilot testing

This certification does NOT prove:
- Bug-free operation under all conditions
- Compatibility with all operating systems
- Availability with all providers
- Ability to complete projects of any scale
- No degradation over months of operation

## Next Steps

1. Obtain human approval for real provider pilot (`--execute-real-runtime`)
2. Execute Phase 13 with 3 representative Goals through claude-default-deepseek
3. 8–24 hour extended soak test
4. Real project pilot
5. Release candidate versioning

---

*Report generated 2026-08-04 by system-release-acceptance runner v0.1.0*
