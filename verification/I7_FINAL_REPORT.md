# I7 Final Report: Runtime Root-Cause Repair

**Date**: 2026-07-26
**I7 Acceptance Code HEAD**: `1c35e5e0eb6ef52ad364ad2d8c3d9a95b87e389d`
**Previous Acceptance Code HEAD**: `5f53a78e236b14d6079a9cdaeb374127c23d5019`

---

## Verdict

**APPROVAL REQUIRED — I7 repaired real runtime acceptance is ready for re-execution.**

Three root-cause gaps confirmed and fixed in this round. The acceptance runner must be re-executed with new approval to verify fixes.

---

## Root Cause Closure

| Gap | Finding | Root Cause | Fix |
|-----|---------|-----------|-----|
| GAP-A | `PlannerEventCollector.final_result = None` | `ANTHROPIC_API_KEY` filtered by `is_safe_env()`; `env_overrides` was empty | Planner/Evaluator now read and pass ANTHROPIC env vars via `env_overrides` |
| GAP-B | A token=0, B token=0 (no takeover) | A used `state_dir_a="i7-accept-a"`, B used `state_dir_b="i7-accept-b"` — separate ownership domains | Both now use `state_dir="i7-accept-shared"` — shared lease domain |
| GAP-C | Certification PASS despite Phase 4/5 failures | `blocking_findings` always empty; no mandatory criteria enforcement | Per-criterion verdicts with required flag; verdict FAIL when any mandatory check fails |
| RC-K | Runner continues after Phase 5 failure | Error logged but not returned | Phase 5 takeover failure now returns `Err` |

---

## Fixes Applied

1. **GAP-A**: `planner.rs` and `evaluator.rs` now read `ANTHROPIC_API_KEY`, `ANTHROPIC_BASE_URL`, `ANTHROPIC_MODEL`, `NO_PROXY` from parent env and pass via `env_overrides` to ProcessManager.

2. **GAP-B**: `i7_acceptance.rs` Supervisor A and B use shared `state_dir="i7-accept-shared"`. Mandatory token comparison: `B token > A token` returns error if false. Old owner fencing verified.

3. **GAP-C**: `run_certification()` now enforces mandatory criteria: quality gates, migration, E2E, provider smoke invocations, crash/takeover token comparison, error-free phases. `blocking_findings` populated for failures.

---

## Quality Gates

| Gate | Result |
|------|--------|
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace` | PASS (0 failed, 0 ignored) |

---

*I7_ACCEPTANCE_CODE_HEAD: 1c35e5e0eb6ef52ad364ad2d8c3d9a95b87e389d*
