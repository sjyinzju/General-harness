# CLI Architecture v3 — Agent Harness

> **版本**: v3.0
> **日期**: 2026-07-15
> **修订**: IPC client 模型, non-interactive mode, HarnessApi boundary

---

## 1. 架构

```
┌──────────────────────────────────────────────────────┐
│ harness (CLI binary)                                  │
│                                                      │
│ harness run "目标"                                     │
│   → spawns harness-supervisor (独立 headless 进程)     │
│   → 连接到 supervisor IPC socket                      │
│   → 作为 IPC client 渲染 TUI / 输出文本                 │
│                                                      │
│ harness attach {id}                                    │
│   → 连接到 supervisor IPC socket (或启动新 supervisor)  │
│   → 接收状态快照 + 事件流                               │
└──────────────────────────────────────────────────────┘
```

详见: `docs/architecture/process-ownership-model.md`

---

## 2. HarnessApi Trait

```rust
#[async_trait]
pub trait HarnessApi: Send + Sync {
    async fn create_run(&self, objective: &str) -> Result<RunHandle>;
    async fn attach_run(&self, run_id: &str) -> Result<(RunHandle, EventReceiver)>;
    async fn approve(&self, run_id: &str) -> Result<()>;
    async fn reject(&self, run_id: &str, feedback: &str) -> Result<()>;
    async fn pause(&self, run_id: &str) -> Result<()>;
    async fn resume(&self, run_id: &str) -> Result<()>;
    async fn cancel(&self, run_id: &str) -> Result<()>;
    async fn send_feedback(&self, run_id: &str, text: &str) -> Result<()>;
    async fn get_status(&self, run_id: &str) -> Result<RunStatus>;
    async fn get_tasks(&self, run_id: &str) -> Result<Vec<TaskSummary>>;
}
```

CLI 通过 `IpcHarnessClient` (IPC socket) 或 `DirectHarness` (测试中直接调用) 使用此 trait。

---

## 3. 交互式 TUI Shell

### 3.1 Ctrl+C 行为

```
第一次 Ctrl+C:
  → IPC command: pause → supervisor → pause: PAUSE_REQUESTED

第二次 Ctrl+C (3s 内):
  → IPC command: cancel → supervisor → 取消所有 Execution
  → 显示确认提示 → 输入 y → supervisor → project CANCELLED

超过 3s: 重置计数器
```

### 3.2 Detach (Ctrl+D)

```
→ CLI 断开 IPC 连接 → CLI 进程退出
→ Supervisor 继续运行 (不暂停)
→ 写入 supervisor.json (供 re-attach)
```

### 3.3 Re-attach

```
harness attach {run_id}
  → 读取 supervisor.json → 连接 socket
  → 如果 supervisor 不存在 → 启动新 supervisor → reconciliation
```

---

## 4. Non-Interactive Mode

```bash
harness run "目标" --non-interactive [--approve]
```

| Flag | 行为 |
|------|------|
| 无 flag | 输出 plan JSON → 等待 stdin "approve\n" → 执行 → 退出 |
| `--approve` | 自动批准 → 直接执行 → 退出 |
| 输出 | 每行一个 JSON: `{"type":"agent_event",...}` 或 `{"type":"status",...}` |
| 退出码 | 0 (所有 task DONE) / 1 (任何 task FAILED/CANCELLED) |

**Non-interactive 模式不得静默自动审批。** `--approve` flag 必须显式传入。

---

## 5. CI No-TTY 模式

Supervisor 可以在无 IPC socket 模式下启动:

```bash
harness-supervisor --run-id test-001 --plan plan.json --no-ipc
```

CI 测试直接调用 `DirectHarness` (HarnessApi 的内存实现)，完全不需要 TTY。

---

## 6. TUI 隔离规则

```
TUI (harness-cli):
  ✅ 通过 HarnessApi trait 调用
  ✅ 接收 HarnessEvent stream → 渲染
  ❌ 不操作 SQLite
  ❌ 不操作 Git
  ❌ 不操作 AgentAdapter
  ❌ 不直接 spawn 子进程
  ❌ 不依赖 rusqlite, git2, tokio::process

harness-cli/Cargo.toml:
  [dependencies]
  harness-core
  harness-runtime (仅 HarnessApi trait)
  ratatui
  crossterm
  # 无 rusqlite, git2, tokio
```
