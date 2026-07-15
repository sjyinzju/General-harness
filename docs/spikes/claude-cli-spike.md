# Claude CLI Spike Report

> **日期**: 2026-07-15
> **版本**: Claude Code 2.1.210
> **模型**: deepseek-v4-pro (custom provider)
> **平台**: Windows 11

---

## Discovery

| Check | Result |
|-------|--------|
| Executable found | ✅ `C:\Users\shiju\AppData\Roaming\npm\claude.ps1` |
| `claude --version` | ✅ `2.1.210 (Claude Code)` |
| Auth status | ✅ Logged in via api_key (ANTHROPIC_API_KEY) |
| `claude -p --output-format stream-json` | ✅ Works |
| `--permission-mode acceptEdits` | ✅ Supported |
| `--verbose` | ✅ Supported |
| `--add-dir` | ✅ Supported |

## Real Stream-JSON Events Observed

### Event Types

| type | subtype | Description |
|------|---------|-------------|
| `system` | `init` | Session initialization. Contains: `session_id`, `cwd`, `model`, `tools[]`, `mcp_servers[]`, `permissionMode`, `slash_commands[]`, `apiKeySource`, `claude_code_version`, `agents[]`, `skills[]`, `plugins[]`, `capabilities[]`, `uuid` |
| `system` | `thinking_tokens` | Estimated token usage updates. Contains: `estimated_tokens`, `estimated_tokens_delta`, `uuid`, `session_id` |
| `assistant` | — | Agent message. `message.content[]`: `thinking` (with signature), `tool_use` (with id, name, input), `text`. Contains `message.model`, `message.usage` |
| `user` | — | Tool result. `message.content[]`: `tool_result` with `tool_use_id`, `content`, `is_error` (implicit). May have `timestamp` |
| *(expected)* `result` | — | Final result event (not captured in this run — task was interrupted) |

### Key Fields

| Field | Location | Description |
|-------|----------|-------------|
| `session_id` | Top-level, every event | UUID session identifier — key for `--resume` |
| `uuid` | Top-level, every event | Per-event UUID |
| `message.id` | Inside `message` | Per-message UUID (multiple events share same message.id during streaming) |
| `message.model` | Inside `assistant` message | Model name string (e.g., `deepseek-v4-pro`) |
| `message.usage` | Inside `assistant` message | `{input_tokens, output_tokens, cache_read_input_tokens, ...}` |
| `message.content[]` | Inside `message` | Array of content blocks |
| `content[].type` | Content block | `thinking`, `tool_use`, `text`, `tool_result` |
| `content[].thinking` | Thinking block | Thinking text (not full chain-of-thought) |
| `content[].signature` | Thinking block | Signature for the thinking block |
| `tool_use.id` | Tool use block | Tool call ID (e.g., `call_00_I8BLrWtY...`) |
| `tool_use.name` | Tool use block | Tool name (e.g., `Glob`, `Read`, `Write`, `Bash`) |

## Stream-JSON Protocol Details

**Input format** (stdin): One JSON object per line — `{"type":"user","message":{"role":"user","content":"prompt text"}}`

**Output format** (stdout): One JSON object per line — event stream as shown above.

**Stderr**: Warning/error messages. Example: `"Warning: no stdin data received in 3s..."` (this appears when no stdin pipe data arrives within 3 seconds — relevant for Harness: we must send the prompt promptly).

## Unverified

- `--resume` with session_id across process restart
- Structured output (`--json-schema` or similar)
- Budget/cost limit enforcement
- Cancellation behavior (SIGTERM during running task)
- Permission blocking behavior
- Unknown event types (all observed events map to known types)
- ReasoningSummary (not observed — `thinking` blocks are incremental, not summary)

## Recommended AgentEvent Mapping

| Claude stream-json event | AgentEvent |
|--------------------------|------------|
| `system.init` | `SessionStarted { session_id, profile_id }` |
| `assistant` with `content[].text` | `Message { content }` |
| `assistant` with `content[].thinking` | `Progress { summary: thinking_text }` |
| `assistant` with `content[].tool_use` | `ToolCallStarted { tool_name, tool_use_id, tool_input }` |
| `user` with `content[].tool_result` | `ToolCallCompleted { tool_use_id, is_error, content_preview }` |
| `result` event (final) | `Result { content, is_error }` |
| Process exit | `ProcessExited { exit_code }` |
| Unknown `type` | `RawVendorEvent { raw_type, payload }` |
| Session end (synthesized) | `SessionEnded { synthetic: true, abnormal: bool }` |

## Stderr Warnings

Claude CLI emits a warning to stderr when no stdin data arrives within 3 seconds:
```
Warning: no stdin data received in 3s, proceeding without it.
```

**Implication for Harness**: After spawning Claude CLI, the prompt must be written to stdin within 3 seconds to avoid this warning. Harness should write the prompt immediately after spawn.

## Claude CLI Uses Process CWD

`claude -p` operates in the **current working directory** of the process, NOT the `--add-dir` directory. The `--add-dir` flag grants ADDITIONAL directory access beyond cwd. For Harness, we should set the subprocess cwd to the worktree directory.

## B5-B7 Status After Spike

- **B5** (AgentEvent coverage): ✅ Verified for Claude — all expected events map to observed types. `ReasoningSummary` not present in stream (thinking is incremental). `RawVendorEvent` may not be needed for Claude (all types known).
- **B6** (Codex vs Claude mapping): PARTIAL — Claude verified, Codex pending.
- **B7** (--resume reliability): NOT VERIFIED — would need a multi-turn test with session persistence.
