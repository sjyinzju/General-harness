# AgentEvent V1 Candidate

> **基于**: Claude CLI 2.1.210 stream-json 真实数据 + Codex CLI 0.116.0 文档
> **状态**: Candidate (NOT frozen — awaiting Codex config fix for full validation)

---

## 事件清单

| # | 事件 | 必需? | Claude 映射 | Codex 映射 (预期) |
|---|------|:---:|------------|-----------------|
| 1 | `SessionStarted` | ✅ 必需 | `system.init` → session_id, model, tools | session start event |
| 2 | `Message` | ✅ 必需 | `assistant` with `content[].text` | assistant message delta |
| 3 | `Progress` | ⬜ 可选 | `assistant` with `content[].thinking` | reasoning/todo updates |
| 4 | `ReasoningSummary` | ⬜ 可选 | NOT OBSERVED — Claude thinking is incremental | ❓ unknown |
| 5 | `ToolCallStarted` | ✅ 必需 | `assistant` with `content[].tool_use` | tool call event |
| 6 | `ToolCallCompleted` | ✅ 必需 | `user` with `content[].tool_result` | tool result event |
| 7 | `Result` | ✅ 必需 | `result` event (final) | turn complete event |
| 8 | `Error` | ✅ 必需 | `result` with `is_error:true` | error event |
| 9 | `ProcessExited` | ✅ 必需 | Synthesized by Adapter on process exit | Synthesized |
| 10 | `RawVendorEvent` | ✅ 必需 | Unknown event types — NOT silently dropped | Unknown event types |
| 11 | `SessionEnded` | ✅ 必需 | Synthesized by Adapter (synthetic=true) | Synthesized |

---

## Enriched Fields (per event, added by harness-runtime)

```rust
struct EnrichedAgentEvent {
    execution_id: String,        // UUID — which Execution Attempt
    receive_sequence: u64,       // monotonic per execution
    received_at: DateTime<Utc>,  // Harness receive time (NOT Agent claimed time)
    event: AgentEvent,
}
```

---

## Key Design Decisions (from Spike)

### ReasoningSummary

Claude CLI emits **incremental** thinking blocks (`content[].type: "thinking"`) during execution, NOT a summary at the end. `ReasoningSummary` is therefore an **optional** capability — Adapters may synthesize a summary from accumulated thinking blocks, but are not required to.

### Unknown Events → RawVendorEvent

Claude CLI does emit event types beyond the known list (e.g., future additions). **Must NOT silently drop.** All unrecognized `type` values → `RawVendorEvent { raw_type, payload }`.

### SessionEnded — Synthetic

Claude CLI does NOT emit a "session_ended" event. The Adapter must **synthesize** this event when:
- `result` event is received (normal completion → `abnormal: false`)
- Process exits without `result` (crash/timeout → `abnormal: true`)
- Supervisor disconnects (LOST → `abnormal: true`)

### ProcessExited — Separate from Result

Claude CLI `result` event does NOT include exit code. `ProcessExited` (exit_code, signal) is a **separate** event, synthesized by Adapter when the subprocess exits.

### Receive Sequence

`receive_sequence` is assigned by harness-runtime at receive time. It exposes ordering, NOT claims ordering. If events arrive out of order from the Agent, the sequence reflects the actual arrival order.

### receive_sequence NOT exposed to AgentEvent

The sequence is in `EnrichedAgentEvent`, not inside `AgentEvent` itself. Adapters do not need to provide it.

### Large Events → File Reference

Events with content > 64KB are written to a file, and the event payload contains `{ content_ref: "path", content_hash: "sha256" }` instead of the full content.

---

## Claude-Specific Observations

### `session_id` is on every event

Every Claude stream-json event contains `session_id`. This is the key for `--resume`.

### `message.id` is shared across streaming chunks

During streaming, multiple events share the same `message.id` while the content blocks accumulate. This is useful for deduplication.

### `message.model` and `message.usage` are informational

Not needed for AgentEvent — these are metadata fields that can be captured in execution audit logs.

### No native `session_ended`

Confirmed: Claude CLI does not emit a terminal session event. The Adapter synthesizes this.

### Stderr warning at 3s idle

Claude CLI warns on stderr if no stdin data arrives within 3 seconds. Harness must write prompt immediately after spawn.

---

## Codex-Specific (Unverified)

Based on `codex exec --json --help` and Codex SDK docs:

### No `--cwd` flag

Codex CLI uses the process cwd. Harness must set the subprocess cwd to the worktree directory.

### JSONL format expected

`codex exec --json` produces JSONL on stdout. Format likely includes thread_id, turn events, tool calls, and final result.

### `thread_id` for resume

Codex uses `thread_id` (not `session_id`) for session continuity.

---

## B5-B7 Final Status

| # | Question | Status |
|---|---------|:---:|
| B5 | AgentEvent coverage verified by real spike? | PARTIAL — Claude ✅, Codex ❌ (config block) |
| B6 | Claude stream-json ↔ Codex JSONL mapping validated? | PARTIAL |
| B7 | `--resume` / thread resume reliability? | NOT VERIFIED |

These remain **Tech Spike Questions** — not Gate A blockers. They will be answered when Codex config is fixed and resume tests are run.
