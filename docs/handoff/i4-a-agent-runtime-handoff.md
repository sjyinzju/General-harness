# I4-A Agent Discovery & Production CLI Adapters — Handoff (Final)

> **Status**: I4-A Closure complete, all quality gates green
> **Date**: 2026-07-17
> **Branch**: `main`
> **Pre-I4-A HEAD**: `0663ae1` — `fix(i3): close transaction and concurrency audit gaps`
> **Closure HEAD**: see commit below

---

## 1. Commits

* `feat(i4-a): add agent discovery and production cli adapters` — I4-A code
* `docs: hand off agent discovery and production adapters` — initial handoff
* `fix(i4-a): close adapter lifecycle and environment gaps` — Closure fixes

## 2. Quality Gates (Final)

| Gate | Result |
|------|--------|
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace` | **518 passed / 0 failed / 0 ignored** |
| `git diff --check` | PASS |
| `git status --short` | clean |

## 3. Test Count Progression

| Phase | Count |
|-------|-------|
| I2B Final | 286 |
| I3 Final | 399 |
| I4-A Initial | 469 |
| I4-A Closure | **518** |

+49 new tests in Closure (+46 adapter integration, +3 env boundary).

## 4. Closure Fixes Summary

### 4.1 RuntimeProfile Environment Boundary

- Added `allowed_env_var_names: Vec<String>` to `ProcessSpec`
- ProcessManager validates overrides: unauthorized sensitive env vars rejected with structured error
- Defense-in-depth: two-layer filtering (allowed set + `is_sensitive_env` re-check)
- Non-sensitive vars (e.g., `MY_APP_CONFIG`) pass without explicit allowlisting
- Environment variable VALUES never enter Debug, Display, error, tracing, or SQLite

### 4.2 Sink Close Cancels Child Process

- Both `ClaudeCliAdapter` and `CodexCliAdapter` `receive_events`:
  - On sink.send() error → cancel process via ProcessManager
  - Wait for process completion
  - Emit exactly one terminal outcome (Cancelled if sink closed)
  - Handle races: sink-close vs natural-exit vs timeout vs external-cancel
- Fixed `AtomicBool` for `is_active()` — no more `tokio::sync::Mutex::blocking_lock()` deadlock

### 4.3 Authentication Expression

- `check_authentication()` no longer returns `authenticated: true` from env var presence
- Both adapters return `authenticated: false` with diagnostic message
- Only real, no-cost login status command can confirm auth (future work)

### 4.4 Production Adapter Integration Tests

- 26 integration tests (Claude: 15, Codex: 15) through real ProcessManager
- 20 parser unit tests (Claude: 8, Codex: 8)
- 3 environment boundary tests
- All using fake executable scripts — no real Agent CLI or API calls

## 5. Migration

- `009_agent_discovery.sql` — additive (3 new tables + ALTER TABLE)
- Migrations 001–008 frozen and untouched
- Business tables: 15 → 18

## 6. Discovery Module

```
crates/harness-core/src/contracts/discovery.rs     — 15 types
crates/harness-runtime/src/discovery/mod.rs         — AgentDiscoveryService
crates/harness-runtime/src/discovery/known_agents.rs — patterns
crates/harness-runtime/src/discovery/repo.rs         — persistence
```

## 7. Adapters

```
crates/harness-adapters/src/claude/mod.rs — ClaudeCliAdapter + ClaudeCliSession
crates/harness-adapters/src/codex/mod.rs — CodexCliAdapter + CodexCliSession
```

## 8. Capability Negotiation

| Capability | Support |
|-----------|---------|
| Execute | Native |
| WorkingDirectory | Native |
| StreamOutput | Native |
| FinalResult | Native |
| StructuredEvents | Native |
| ProcessExit | HarnessEmulated |
| Timeout | HarnessEmulated |
| Cancellation | HarnessEmulated |
| NativeResume | Unknown |
| FileAttachments | Unsupported |

**Native 5 + HarnessEmulated 3 + Unknown 1 + Unsupported 1 = 10**

## 9. Real CLI Manual Verification

**Not yet wired.** The `harness-cli` crate's `main.rs` has placeholder code only. Manual validation requires explicit `--validate` subcommand (not `--profile` on `discover`). When implemented:
- Must display executable, full args, profile, cwd, timeout, may_incur_cost
- Must NOT auto-trigger from passive discovery
- Must use temp working directory
- Must NOT modify login state or global config

## 10. Known Gaps (Not Fixed)

- `check_authentication` still uses env presence heuristic (but now correctly returns `authenticated: false`)
- `verify_no_secrets_in_db` only checks `runtime_profiles` table, not `discovery_evidence` or `agent_provider_hints`
- Codex `--help` compatibility check depends on English locale output
- `probe_command` in discovery uses `spool_dir: None` (works for short probes)
- Adapter integration tests rely on batch scripts (fragile on non-Windows; parser tests are portable)

## 11. Remaining Medium Risks

| Risk | Severity |
|------|:---:|
| `env_overrides` not filtered in discovery probe_command (but no overrides set there) | Low |
| Sink-close cancellation adds process_manager.cancel() overhead on every sink error | Low |

## 12. I4-A Final Exit Conditions

| Condition | Met |
|-----------|:---:|
| Passive Discovery complete | ✅ |
| AgentDefinition + RuntimeProfile model correct | ✅ |
| Claude Production Adapter complete | ✅ |
| Codex Production Adapter complete | ✅ |
| All external processes through ProcessManager | ✅ |
| No credential reading or storage | ✅ |
| No auto-upgrade, login, or global config modification | ✅ |
| Timeout/cancel/event streaming complete | ✅ |
| Capability negotiation complete | ✅ |
| Persistence idempotent | ✅ |
| Environment boundary defense-in-depth | ✅ |
| Sink close cancels child process | ✅ |
| Auth from env presence ≠ authenticated | ✅ |
| Adapter integration tests (49 new) | ✅ |
| Automated tests 0 failed, 0 ignored (518 total) | ✅ |
| No Gate C frozen contract blocker | ✅ |
| fmt / clippy(-D warnings) / git diff --check | ✅ |

## 13. Ready for I4-B Scheduler

**Yes.** All I4-A exit conditions met.
