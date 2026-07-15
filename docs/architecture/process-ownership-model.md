# Process Ownership Model — Agent Harness

> **版本**: v1.0
> **日期**: 2026-07-15
> **状态**: 待审批

---

## 1. 模型总览

Harness 使用**独立 Headless Supervisor 进程**模型。

```
┌──────────────────────────────────────────────────────────┐
│                    harness (CLI binary)                   │
│                                                          │
│  harness run "目标"                                       │
│    └─→ spawns harness-supervisor (独立子进程)              │
│        └─→ supervisor 写入 .harness/runs/{id}/supervisor.json │
│                                                          │
│  harness attach {id}                                      │
│    └─→ 连接到 supervisor 的 IPC socket                    │
│        └─→ 作为 IPC client 接收状态 + 事件                  │
│                                                          │
│  TUI/CLI 是 supervisor 的 IPC client。                     │
│  Supervisor 内没有 TUI 线程。                              │
│  Supervisor 内没有 ratatui 依赖。                          │
└──────────────────────────────────────────────────────────┘
```

## 2. Supervisor 进程

### 2.1 职责

Supervisor 是**唯一**管理 Agent 子进程、SQLite、Git 和状态机的进程。

- 从 SQLite 读取/写入项目状态
- 创建和管理 Agent 子进程 (通过 ProcessManager)
- 管理 Git worktree
- 接收并持久化 AgentEvent
- 执行验证和提交
- 向 IPC clients 广播 HarnessEvent

### 2.2 启动

```bash
# 用户不可见 — 由 harness CLI 自动启动
harness-supervisor --run-id proj-001
```

### 2.3 生命周期

```
spawned by CLI
  │
  ▼
INITIALIZING
  │ 从 SQLite 恢复状态 (或创建新 Project)
  ▼
RUNNING
  │ 接受 IPC 连接, 管理 Agent, 处理任务
  │
  ├─→ (detach)   CLI client 断开 → supervisor 继续 RUNNING
  ├─→ (attach)   新 CLI client 连接 → supervisor 发送状态快照
  ├─→ (complete) 所有任务完成 → 通知 clients → 等待所有 client 断开
  ├─→ (crash)    进程终止 → Agent 子进程被 OS/watchdog 清理
  └─→ (shutdown) 正常退出
```

## 3. IPC 协议

### 3.1 Transport

| 平台 | 机制 | 地址 |
|------|------|------|
| Unix (macOS/Linux) | Unix domain socket | `.harness/runs/{run_id}/supervisor.sock` |
| Windows | Named pipe | `\\.\pipe\harness-{run_id}` |

### 3.2 协议格式

JSON lines (每行一个完整 JSON 对象，`\n` 分隔)，双向。

**Supervisor → Client**:

```json
{"type":"state_snapshot","payload":{"project":{...},"tasks":[...]}}
{"type":"harness_event","payload":{"event_type":"task_status_changed","task_id":"...","from":"...","to":"..."}}
{"type":"harness_event","payload":{"event_type":"agent_event","execution_id":"...","agent_event":{...}}}
{"type":"heartbeat","timestamp":"..."}
```

**Client → Supervisor**:

```json
{"type":"command","command":"approve","payload":{}}
{"type":"command","command":"pause"}
{"type":"command","command":"resume"}
{"type":"command","command":"cancel"}
{"type":"command","command":"feedback","payload":{"text":"修改意见"}}
```

### 3.3 Supervisor 元数据文件

`.harness/runs/{run_id}/supervisor.json`:

```json
{
  "run_id": "proj-001",
  "pid": 12345,
  "started_at": "2026-07-15T10:00:00Z",
  "ipc_socket": ".harness/runs/proj-001/supervisor.sock",
  "status": "RUNNING"
}
```

## 4. Supervisor 崩溃恢复

### 4.1 核心原则

**不能重新接管仍存活的 Agent CLI 子进程。**

原因: Agent CLI (Claude Code, Codex) 的 stdin/stdout pipe 在 supervisor 崩溃时断开。新 supervisor 无法重新连接到已断开的 pipe。Agent 子进程可能在等待 stdin 输入，或者 stdout 写入失败 (SIGPIPE)。

### 4.2 崩溃恢复流程

```
Supervisor 崩溃
  │
  ├─ Agent 子进程 (通过 Job Object/进程组管理):
  │    ├─ 被 watchdog 检测到 supervisor 失效
  │    └─ 被 OS 终止 (Job Object 自动清理 / watchdog SIGKILL)
  │
  ├─ 数据库状态:
  │    └─ SQLite WAL 保证最后一个 committed 事务安全
  │
  └─ 用户执行 harness attach {run_id}:
       │
       ▼
     1. 检测 supervisor.json 中 PID 不存在
     2. 新 supervisor 启动
     3. 从 SQLite 恢复状态
     4. Reconciliation 检测:
        - 所有处于 RUNNING 的 Execution Attempt → 标记为 LOST
        - 所有处于 ACQUIRED 的 WorkspaceLease → 检查 worktree 是否存在
     5. 对每个 LOST Execution:
        a. retry_count < max → 创建新 Execution Attempt + --resume (使用保存的 session_id)
        b. retry_count ≥ max → Execution FAILED → Task FAILED
     6. 新 supervisor 写入自己的 supervisor.json
     7. 接受 IPC 连接 → 正常继续
```

### 4.3 LOST vs ORPHANED

```
LOST:      Supervisor 崩溃导致的 Execution 丢失。
           已知原因: supervisor 进程终止。
           处理: 创建新 Execution + --resume（使用保存的 native_session_id）。

ORPHANED:  Agent 子进程异常退出且 supervisor 仍在运行。
           已知原因: Agent crash / OOM / 超时被 kill。
           处理: retry 或 fail。
```

## 5. Agent 子进程管理

### 5.1 进程树

```
supervisor (PID 1000)
  │
  ├─ Agent 子进程 (PID 1001) ← Claude CLI
  │   └─ (Claude 自身的子进程)
  │
  └─ Agent 子进程 (PID 1002) ← Codex CLI
      └─ (Codex 自身的子进程)
```

### 5.2 平台终止机制

**Windows**: Job Object
```
CreateJobObject → 所有 Agent 子进程分配到该 Job Object
Supervisor 崩溃 → Job Object 引用计数归零 → OS 自动终止所有子进程
```

**Unix**: 进程组
```
setpgid(agent_pid, supervisor_pid)  // 或独立进程组
Supervisor 崩溃 → watchdog 子进程检测父进程死亡 → kill(-pgid, SIGKILL)
```

### 5.3 Watchdog 子进程

Supervisor 启动时 spawn 一个轻量 watchdog 子进程:
- 唯一职责: 监控父进程 (supervisor) 是否存活
- 父进程死亡 → 终止 Agent 进程组 → 退出
- 不操作 SQLite、不操作 Git、不接受 IPC
- 极简——减少自身崩溃风险

## 6. IPC Client (CLI/TUI)

### 6.1 职责

- 连接到 supervisor 的 IPC socket
- 渲染 TUI (如果交互式) 或输出文本 (如果非交互)
- 将用户输入转换为结构化 IPC command
- 不操作 SQLite、Git、Adapter、子进程

### 6.2 Detach

```
用户 Ctrl+D → CLI 断开 socket 连接 → CLI 进程退出
Supervisor 检测到 client 断开 → 继续管理 Agent
Supervisor 状态不变 (不暂停)
```

### 6.3 Re-attach

```
harness attach {run_id}
  → 检测 .harness/runs/{run_id}/supervisor.json
  → PID 存在 → 连接到 socket → 接收状态快照 → 渲染 TUI
  → PID 不存在 → 启动新 supervisor → reconciliation → 接收状态快照 → 渲染 TUI
```
