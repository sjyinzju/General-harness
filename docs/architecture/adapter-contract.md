# Adapter Contract v3 — Agent Harness

> **版本**: v3.0
> **日期**: 2026-07-15
> **修订**: AgentEvent v2, CodexCliAdapter, Contract Freeze 推迟到 Gate C

---

## 1. AgentAdapter Trait (不变)

```rust
#[async_trait]
pub trait AgentAdapter: Send + Sync {
    fn kind(&self) -> &'static str;
    async fn detect(&self, binary_path: Option<&Path>) -> Result<DetectionResult>;
    async fn get_version(&self) -> Result<String>;
    async fn inspect_configuration(&self) -> Result<AgentConfigInfo>;
    async fn check_authentication(&self) -> Result<AuthCheckResult>;
    async fn probe(&self, temp_dir: &Path) -> Result<ProbeResult>;
    async fn start_session(&self, profile: &RuntimeProfile, opts: &SessionOptions) -> Result<Box<dyn AgentSession>>;
}

#[async_trait]
pub trait AgentSession: Send {
    fn session_id(&self) -> &str;
    fn is_active(&self) -> bool;
    async fn send_task(&mut self, envelope: &TaskEnvelope) -> Result<()>;
    async fn receive_events(&mut self) -> Result<mpsc::Receiver<AgentEvent>>;
    async fn interrupt(&self) -> Result<()>;
    async fn cancel(&self) -> Result<()>;
    async fn dispose(&mut self) -> Result<()>;
}
```

---

## 2. AgentEvent v2

```rust
/// 由 Adapter 发出 (Agent 原生事件)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AgentEvent {
    #[serde(rename = "session_start")]
    SessionStart { session_id: String, profile_id: String },

    #[serde(rename = "assistant_message")]
    AssistantMessage { content: String },

    /// 可选的进度摘要 (替代 Thinking — 不是必需事件)
    #[serde(rename = "progress")]
    Progress { summary: String },

    /// Agent 完成后的推理摘要 (可选)
    #[serde(rename = "reasoning_summary")]
    ReasoningSummary { summary: String },

    #[serde(rename = "tool_use")]
    ToolUse { tool_name: String, tool_input: Value, tool_use_id: String },

    #[serde(rename = "tool_result")]
    ToolResult { tool_use_id: String, is_error: bool, content: String },

    #[serde(rename = "error")]
    Error { message: String, code: Option<String> },

    #[serde(rename = "result")]
    Result { content: String, is_error: bool },

    /// Agent 子进程已退出 (新增)
    #[serde(rename = "process_exited")]
    ProcessExited { exit_code: i32, signal: Option<i32> },

    /// 原始 vendor 事件透传 (新增 — 未知事件不静默丢弃)
    #[serde(rename = "raw_vendor_event")]
    RawVendorEvent { raw_type: String, payload: Value },

    #[serde(rename = "session_end")]
    SessionEnd {
        session_id: String,
        synthetic: bool,   // 是否由 Adapter 合成 (非 Agent 原生)
        abnormal: bool,    // 是否异常终止
    },
}

/// 由 harness-runtime 封装的 enriched 事件
#[derive(Debug, Clone)]
pub struct EnrichedAgentEvent {
    pub execution_id: String,
    pub receive_sequence: u64,   // 单调递增 (per execution)
    pub received_at: DateTime<Utc>,
    pub event: AgentEvent,
}
```

### 2.1 关键语义

| 场景 | 事件序列 |
|------|---------|
| 正常完成 | ... → Result → ProcessExited(exit_code=0) → SessionEnd(synthetic=false, abnormal=false) |
| Agent 异常退出 | ... → ProcessExited(exit_code≠0) → SessionEnd(synthetic=true, abnormal=true) |
| Supervisor 崩溃 | 无更多事件 (pipe 断开) → Execution LOST |
| Unknown vendor event | → RawVendorEvent (不静默丢弃) |
| 乱序到达 | receive_sequence 暴露乱序，接收时间不声称解决 |
| 重复 tool_use_id | Warning 日志 + 不重复转发 |
| 断流 N 秒 | 检查进程存活 → 存活着继续等待 → 不存活 → ProcessExited + SessionEnd(abnormal=true) |

---

## 3. Foundation Adapters

### FakeAgentAdapter

- 完全可脚本化控制
- 通过 contract test suite

### ClaudeCliAdapter

- 子进程: `claude -p --input-format stream-json --output-format stream-json`
- 解析 JSONL → AgentEvent

### CodexCliAdapter (命名修正)

- 子进程: codex CLI JSON-RPC over stdio
- 解析 JSON-RPC → AgentEvent
- 命名: `CodexCliAdapter` (非 CodexAppServerAdapter)

---

## 4. Contract Freeze 策略

```
Gate B (Contract Candidate):
  - 所有 type + trait 定稿为 candidate
  - FakeAdapter 通过 contract tests

Gate B → Gate C:
  - Codex CLI tech spike (真实 JSON-RPC 验证)
  - Claude CLI tech spike (真实 stream-json 验证)
  - 验证 AgentEvent v2 覆盖所有必要事件

Gate C (Contract Freeze v1):
  - AgentAdapter v1 冻结
  - AgentEvent v1 冻结
  - 之后通过 migration 演进
```
