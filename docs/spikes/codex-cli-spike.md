# Codex CLI Spike Report

> **日期**: 2026-07-15
> **版本**: Codex CLI 0.116.0
> **平台**: Windows 11

---

## Discovery

| Check | Result |
|-------|--------|
| Executable found | ✅ `C:\Users\shiju\AppData\Roaming\npm\codex.ps1` |
| `codex --version` | ✅ `codex-cli 0.116.0` |
| `codex exec --help` | ✅ Supported |
| `codex login status` | ❌ Blocked by config.toml validation |
| `codex exec --json` | ❌ Blocked by config.toml |

## Blocking Issue

```
Error loading configuration: C:\Users\shiju\.codex\config.toml:5:16:
unknown variant `default`, expected `fast` or `flex` in `service_tier`
```

This is a **user configuration issue** — the `service_tier` field in `~/.codex/config.toml` has value `default` which is not recognized by Codex CLI 0.116.0. Valid values are `fast` or `flex`.

Resolution: User must edit `~/.codex/config.toml` to fix the `service_tier` value. This is NOT a Harness bug.

## Verified Capabilities (from --help)

| Capability | Supported? |
|-----------|:---:|
| Non-interactive execution | ✅ `codex exec --json` |
| JSONL stdout output | ✅ (inferred from `--json` flag) |
| Separate stderr | ✅ (standard subprocess behavior) |
| Resume | ✅ `codex resume` (interactive) |
| MCP server mode | ✅ `codex mcp-server` |
| Sandbox execution | ✅ `codex sandbox` |
| Git apply | ✅ `codex apply` |
| Working directory control | ❌ No `--cwd` flag (uses process cwd) |
| Structured output (JSON Schema) | ❓ Not confirmed (no docs found) |
| Budget/cost limit | ❓ Not confirmed |

## Unverified (due to config block)

- Actual JSONL event format
- Session/thread ID field names
- Normal completion event structure
- Error/failure event structure
- Timeout behavior
- Cancellation behavior (SIGTERM response)
- Native resume across new processes
- Permission/approval flow
- Structured output format

## Recommended AgentEvent Mapping (tentative)

Based on documented `codex exec --json` format and Codex SDK documentation:

| Codex JSONL field | AgentEvent |
|------------------|------------|
| `session.start` | `SessionStarted` |
| `assistant.message` | `Message` |
| `tool.call` / `tool.use` | `ToolCallStarted` |
| `tool.result` | `ToolCallCompleted` |
| `turn.complete` | `Result` |
| Process exit | `ProcessExited` |
| Unknown event type | `RawVendorEvent` |

## B5-B7 Status After Spike

- **B5** (Tech spike verifying AgentEvent coverage): PARTIAL — Codex actual output not captured due to config block
- **B6** (Codex JSONL vs Claude stream-json mapping): PARTIAL — Claude verified, Codex pending config fix
- **B7** (--resume reliability): NOT VERIFIED — requires working execution to test

## Next Steps

1. User fixes `~/.codex/config.toml` service_tier
2. Re-run `codex exec --json "simple task"` to capture real JSONL
3. Save sanitized event samples to `tests/fixtures/codex-cli/`
4. Verify EventAgent mapping against real data
