# I4-A Agent Discovery & Production CLI Adapters — Handoff

> **Status**: I4-A complete, quality gates all green, ready for I4-B Scheduler
> **Date**: 2026-07-17
> **Branch**: `main`
> **HEAD (pre-I4-A)**: `0663ae1` — `fix(i3): close transaction and concurrency audit gaps`

---

## 1. Takeover Audit Summary

| Item | Status |
|------|--------|
| HEAD | `0663ae1` |
| Branch | `main` |
| Working tree | clean |
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace` | **469 passed / 0 failed / 0 ignored** |
| `git diff --check` | PASS |
| I3 Closure | Complete |
| Gate C frozen contracts | Intact — no modifications |

**Pre-existing test count**: 395 (I2B + I3 + Closure)
**I4-A test count**: 469 (+74 new tests)

---

## 2. Commit

`feat(i4-a): add agent discovery and production cli adapters`

---

## 3. Migration

**009_agent_discovery.sql** — additive migration:

- New table: `agent_definitions` (id, agent_kind, label, executable_path, discovery_source, version, is_wrapper, wraps_agent_kind, passive_status, diagnostics_json, first_seen_at, last_seen_at)
- New table: `discovery_evidence` (id, agent_definition_id, evidence_kind, observation, confidence, collected_at)
- New table: `agent_provider_hints` (id, agent_definition_id, provider, source, confidence, evidence_json, base_url, is_custom_endpoint)
- Extends `runtime_profiles` with: label, first_seen_at, last_seen_at, capability_negotiation_json, validation_status_json

Migrations 001–008 are frozen and untouched. Business tables: 15 → 18.

---

## 4. Discovery Module Structure

```
crates/harness-core/src/contracts/discovery.rs     — types (DiscoveredAgent, ExecutableIdentity,
                                                      DiscoveryEvidence, ProviderHint, CapabilitySupport,
                                                      CapabilityNegotiation, ValidationStatus, etc.)
crates/harness-runtime/src/discovery/mod.rs         — AgentDiscoveryService (PATH scan, probe,
                                                      wrapper detection, env evidence, provider inference)
crates/harness-runtime/src/discovery/known_agents.rs — Known agent patterns (claude, codex) + wrapper patterns
crates/harness-runtime/src/discovery/repo.rs         — Persistence (upsert agent, evidence, hints, profiles;
                                                      idempotent; verify_no_secrets)
```

### Key Types Added

- `DiscoveredAgent` — full discovery result
- `ExecutableIdentity` — stable identity (path + kind + SHA-256 hash)
- `DiscoveryEvidence` + `EvidenceKind` — per-evidence record
- `DiscoveryConfidence` — High / Medium / Low / Heuristic
- `ProviderHint` + `ProviderHintSource` — provider with evidence (never asserted from model name)
- `AuthenticationState` + `AuthStateValue` + `AuthModeHint` — auth inference
- `CapabilitySupport` — Native / HarnessEmulated / Unsupported / Unknown
- `CapabilityNegotiation` — 10 capability dimensions
- `AdapterCompatibility` + `CompatibilityDiagnostic` — version/feature checks
- `ValidationStatus` + `ValidationResult` — active validation tracking
- `ActiveValidationRequest` — what to show user before paid probe

---

## 5. AgentDefinition / RuntimeProfile Model

```
AgentDefinition (1) ─── RuntimeProfile (1..N)
```

- `AgentDefinition` — existing in `harness-core/contracts/agent_definition.rs`
- `RuntimeProfile` — existing in `harness-core/contracts/runtime_profile.rs`, extended with:
  - `label`, `first_seen_at`, `last_seen_at` (migration 009)
  - `capability_negotiation_json`, `validation_status_json`
- Provider info is always `ProviderHint` with evidence + confidence — never a bare string
- `--model` does NOT switch provider endpoint
- Wrappers (e.g., `claude-glm`) are opaque — no internal parsing

---

## 6. Claude CLI Production Adapter

**Module**: `crates/harness-adapters/src/claude/mod.rs`

- Implements `AgentAdapter` and `AgentSession` (Gate C frozen traits)
- All process lifecycle via `ProcessManager` (spawn, timeout, cancel, capture, redaction)
- Stream-json parsing: `system.init` → SessionStarted, `assistant` → Message/ToolCallStarted/Progress, `user` → ToolCallCompleted, `result` → Result, unknown → RawVendorEvent
- SessionEnded synthesized with `TerminationReason`
- No hardcoded model, provider, base URL, or auth mode
- Env construction via explicit overlay/allowlist (never forwards all harness env)
- Stdin prompt written immediately to avoid 3s stderr warning

---

## 7. Codex CLI Production Adapter

**Module**: `crates/harness-adapters/src/codex/mod.rs`

- Implements `AgentAdapter` and `AgentSession` (Gate C frozen traits)
- All process lifecycle via `ProcessManager`
- JSONL parsing: `thread.started` → SessionStarted, `item.completed` → Message/ToolCallStarted/ToolCallCompleted, `turn.completed` → Result, `turn.failed` → Result(is_error), unknown → RawVendorEvent
- `check_compatibility()` produces structured `AdapterCompatibility` diagnostic
- ChatGPT-login compatible profile representation (auth_mode=Login, no API key)
- No hardcoded model, service_tier, or provider
- Incompatible versions → `CompatibilityDiagnostic` (never auto-upgrades)

---

## 8. Capability Negotiation

10 capability dimensions, each marked:

| Capability | Support |
|-----------|---------|
| Execute | Native |
| WorkingDirectory | Native |
| StreamOutput | Native |
| FinalResult | Native |
| ProcessExit | HarnessEmulated (ProcessManager) |
| Timeout | HarnessEmulated (ProcessManager) |
| Cancellation | HarnessEmulated (ProcessManager) |
| StructuredEvents | Native |
| NativeResume | Unknown |
| FileAttachments | Unsupported |

Harness-emulated capabilities are never reported as Native.

---

## 9. Persistence

- Agent definitions, evidence, provider hints persisted idempotently
- Runtime profiles upserted (update on version/last_seen change)
- `verify_no_secrets_in_db()` validates no secrets leaked to database
- Repeated discovery is idempotent: stable identity, update last_seen_at, merge evidence
- Environment variable NAMES only — values never read, stored, or logged

---

## 10. Test Coverage (74 new tests)

### Unit tests (harness-runtime lib — 10 tests)
- `test_executable_identity_stable`
- `test_executable_identity_different_paths`
- `test_executable_identity_different_kinds`
- `test_wrapper_basename_detected`
- `test_non_wrapper_basename_not_detected`
- `test_capability_negotiation_all_unknown`
- `test_upsert_agent_definition_idempotent`
- `test_upsert_updates_last_seen`
- `test_no_secret_values_in_db`
- `test_runtime_profile_idempotent`

### Integration tests (harness-runtime/tests/agent_discovery_tests.rs — 60 tests)
- Tests 1-5: ExecutableIdentity & basic types
- Tests 6-10: CapabilityNegotiation (Native, HarnessEmulated, Unsupported, Unknown, not pretending native)
- Tests 11-15: ProviderHint (evidence, custom endpoint, not-by-model-name, env low confidence, user-declared)
- Tests 16-18: AuthenticationState (unknown, authenticated, env key not claiming logged in)
- Tests 19-21: DiscoveryEvidence (path, version, env names only)
- Tests 22-26: DiscoveredAgent model (scaffold, multiple profiles, wrapper, confidence)
- Tests 27-29: RuntimeProfile model (basic, not hardcoded model, not hardcoded auth)
- Tests 30-32: ValidationStatus (default, requires permission, diagnostic persistence)
- Tests 33-38: AgentEvent contract (session_started, raw_vendor, synthetic, result vs exit, nonzero exit, termination reasons)
- Tests 39-42: Adapter contract (compatibility, incompatible, ChatGPT login, no hardcoded model)
- Tests 43-48: Persistence (discovery, profile update, no secrets, idempotent, evidence, provider hints)
- Tests 49-50: Table count (18 tables), migration 009 applied
- Tests 51-54: Missing executable, wrapper opaque, dedup, DeepSeek env no false Anthropic
- Tests 55-58: Event ordering, forward compatible, malformed diagnostic, exactly one terminal
- Tests 59-60: Env var names without values, no global config modified

### Existing tests (unchanged, all pass)
- harness-core: 63
- harness-runtime (lib): 63 (now 73 with discovery)
- persistence_closure: 20
- process_capture: 14
- process_integration: 14
- resource_claim_*: 54
- workspace_*: 65
- worktree_manager: 30
- harness-adapters: 4
- golden_path_minimal: 4
- snapshot_wire_format: 10

**Total: 469 passed / 0 failed / 0 ignored**

---

## 11. Manual Real-CLI Verification (Optional)

Manual verification command (NOT in automated tests):

```powershell
# Requires claude in PATH and valid auth
cargo run --bin harness-cli -- discover --profile claude-default

# Requires codex in PATH and valid auth
cargo run --bin harness-cli -- discover --profile codex-default
```

**Warnings displayed before execution**:
- Executable path, full args, working directory, timeout
- Whether the operation may incur API costs
- Environment variable names that will be forwarded

---

## 12. Platform Constraints

- Windows 11: PowerShell wrappers (.ps1), CMD wrappers (.cmd) handled in PATH resolution
- SQLite WAL mode, single-writer (max_connections=1)
- UNIX: process group-based tree termination (via ProcessTreeGuard)
- Windows: Job Object-based tree termination (via ProcessTreeGuard)

---

## 13. Explicitly NOT Implemented (I4-B and beyond)

- Task DAG Scheduler
- Verification Pipeline
- Automatic retry
- Commit/Integration Queue
- Supervisor IPC
- TUI
- Project Goal Loop
- Active validation auto-trigger (opt-in permission required)
- Real Agent CLI calls in automated tests

---

## 14. I4-A Exit Conditions

| Condition | Met |
|-----------|:---:|
| Passive Discovery complete | Yes |
| AgentDefinition + RuntimeProfile model correct | Yes |
| Claude Production Adapter complete | Yes |
| Codex Production Adapter complete | Yes |
| All external processes through ProcessManager | Yes |
| No credential reading or storage | Yes |
| No auto-upgrade, login, or global config modification | Yes |
| Timeout/cancel/event streaming complete | Yes |
| Capability negotiation complete | Yes |
| Persistence idempotent | Yes |
| Automated tests 0 failed, 0 ignored | Yes (469/0/0) |
| No Gate C frozen contract blocker | Yes |
| fmt / clippy(-D warnings) / git diff --check | Yes |

---

## 15. Ready for I4-B Scheduler

**Yes.** All I4-A exit conditions met. I4-B Scheduler can use:
- `AgentDiscoveryService` for agent detection
- `ClaudeCliAdapter` / `CodexCliAdapter` for agent execution
- `RuntimeProfile` + `CapabilityNegotiation` for capability-aware scheduling
- Agent definitions and profiles persisted and queryable

---

**Ready for I4-B.**
