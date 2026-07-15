# AgentAdapter V1 Candidate

> **基于**: Process Supervisor Spike + Claude CLI Spike + Codex CLI (文档)
> **状态**: Candidate (NOT frozen)

---

## Trait 定义

```rust
#[async_trait]
pub trait AgentAdapter: Send + Sync {
    fn kind(&self) -> &'static str;

    // ── Discovery ────────────────────────────────
    async fn detect(&self, binary_path: Option<&Path>) -> Result<DetectionResult, String>;
    async fn get_version(&self) -> Result<String, String>;
    async fn inspect_configuration(&self) -> Result<AgentConfigInfo, String>;
    async fn check_authentication(&self) -> Result<AuthCheckResult, String>;
    async fn probe(&self, temp_dir: &Path) -> Result<ProbeResult, String>;

    // ── Execution ────────────────────────────────
    async fn start_session(
        &self, profile: &RuntimeProfile, opts: &SessionOptions,
    ) -> Result<Box<dyn AgentSession>, String>;
}

#[async_trait]
pub trait AgentSession: Send {
    fn session_id(&self) -> &str;
    fn is_active(&self) -> bool;
    async fn send_task(&mut self, envelope: &TaskEnvelope) -> Result<(), String>;
    async fn receive_events(
        &mut self, on_event: &(dyn Fn(AgentEvent) + Send + Sync),
    ) -> Result<(), String>;
    async fn interrupt(&self) -> Result<(), String>;
    async fn cancel(&self) -> Result<(), String>;
    async fn dispose(&mut self) -> Result<(), String>;
}
```

---

## CapabilitySet (Required + Optional)

### Required (all adapters MUST support)

| Capability | Meaning | Verified (Claude) | Verified (Codex) |
|-----------|---------|:---:|:---:|
| `execute` | Can execute tasks | ✅ | ❌ config block |
| `working_directory` | Respects cwd | ✅ (process cwd) | ✅ (process cwd) |
| `stream_output` | Streaming stdout events | ✅ stream-json | ✅ JSONL |
| `process_exit` | Reports exit code | ✅ via ProcessExited | ✅ via ProcessExited |
| `cancellation` | Responds to SIGTERM/kill | ✅ (process kill) | ❓ |
| `timeout` | Enforces time limits | ❓ (Claude internal) | ❓ |
| `final_result` | Returns structured result | ✅ `result` event | ✅ turn complete |

### Optional (detected via Probe)

| Capability | Claude | Codex | Notes |
|-----------|:---:|:---:|------|
| `native_session_resume` | ✅ `--resume {session_id}` | ✅ `codex resume` | |
| `structured_output` | ❓ | ❓ | Not confirmed for either |
| `tool_events` | ✅ tool_use/tool_result | ✅ | |
| `file_change_events` | ❌ | ❌ | Neither emits explicit file change events |
| `reasoning_summary` | ❌ (incremental only) | ❓ | |
| `interactive_approval` | ✅ (permission prompts) | ❓ | |
| `usage_reporting` | ✅ `message.usage` | ❓ | |

---

## Spike-Verified Behavior

### Claude CLI

- Spawn: `claude -p --input-format stream-json --output-format stream-json --verbose --permission-mode acceptEdits`
- cwd: Set to worktree directory (uses process cwd, not `--add-dir`)
- stdin: Write prompt JSON immediately (within 3s to avoid stderr warning)
- stdout: Parse JSONL lines → AgentEvent
- stderr: Capture separately → log warnings
- session_id: Extract from `system.init` event → store for `--resume`
- SessionEnd: Synthesize when `result` event arrives or process exits

### Codex CLI

- Spawn: `codex exec --json "{prompt}"` (cwd set to worktree)
- stdout: Parse JSONL → AgentEvent
- stderr: Capture separately
- thread_id: Extract → store for resume
- Blocked by config.toml issue — actual event format UNVERIFIED

---

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Agent binary not found | `detect()` → `found: false` |
| Agent not authenticated | `check_authentication()` → `authenticated: false` |
| Agent process exits with error | `AgentEvent::Error` + `AgentEvent::ProcessExited(exit_code≠0)` |
| Timeout | `interrupt()` → wait → `cancel()` → `ProcessExited` |
| Supervisor crash | Process killed by watchdog → Execution LOST |
| Unknown event type | `AgentEvent::RawVendorEvent` (NOT silently dropped) |
| Malformed JSON line | Log warning, continue reading |
| Stdin pipe broken | Agent process exits → captured via `ProcessExited` |

---

## Differences Between Adapters

These differences MUST be expressed through `CapabilitySet`, NOT hardcoded agent-specific logic:

| Difference | Claude | Codex |
|-----------|--------|-------|
| Session ID field | `session_id` | `thread_id` |
| Resume flag | `--resume {id}` | resume via thread |
| Streaming protocol | stream-json (type tags) | JSONL (type tags TBD) |
| Stderr behavior | Warning at 3s idle | TBD |
| Thinking/Reasoning | Incremental thinking blocks | TBD |

The Adapter encapsulates all these differences. Harness-core NEVER sees `session_id` vs `thread_id` — it only sees `AgentSession.native_session_id()`.
