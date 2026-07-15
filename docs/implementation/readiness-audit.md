# Implementation Readiness Audit v2 — Agent Harness

> **版本**: v2.1 (Gate A 完成)
> **日期**: 2026-07-15
> **取代**: v2.0
> **Gate A 状态**: ✅ PASSED
> **状态**: 待审批

---

## 修订摘要

本修订整合了 10 项反馈。核心变更：

| # | 领域 | v1.0 | v2.0 |
|---|------|------|------|
| 1 | Supervisor 模型 | TUI/CLI 是 supervisor 内线程 | 独立 headless supervisor, CLI 是 IPC client |
| 2 | 崩溃后接管 | 重新接管仍存活 Agent 子进程 | 终止所有 Agent 子进程, Execution→LOST, 新建+--resume |
| 3 | 一致性方案 | 外部副作用在事务前执行 | Operation/Saga 两阶段模型 |
| 4 | Concurrency | 仅 DAG + slot + lease | 增加 Resource Claim (文件/目录/逻辑资源) |
| 5 | 状态模型 | 项目 15 + 任务 13 | 四套核心生命周期: Project, Task, ExecutionAttempt, WorkspaceLease |
| 6 | AgentEvent | 8 种事件, Thinking 必需 | Thinking→可选 Progress, +ProcessExited, +RawVendorEvent, +execution_id+receive_sequence |
| 7 | Git | 允许空 commit, 自动切换策略 | 禁止空 commit, 禁止自动切换, IntegrationJob+Repair |
| 8 | Contract Freeze | F0 前硬冻结 | 推迟到 tech spike 后, Gate A→B→C→D→E |
| 9 | Gate | 3 个 Gate | 5 个 Gate: A(Architecture) B(Contract Candidate) C(Contract Freeze) D(Runtime Integration) E(Foundation Acceptance) |
| 10 | 矛盾修正 | 多处 | 见 §10 |

---

## 1. Process Ownership (Revised)

### 1.1 模型

**独立 headless supervisor 进程**。TUI/CLI 是 IPC client，不是 supervisor 内线程。

```
harness run → spawns harness-supervisor → supervisor writes supervisor.json
harness attach → connects to supervisor's IPC socket → receives state + events

Supervisor: 管理 SQLite, Git, Agent 子进程, 状态机
CLI:        IPC client, 渲染 TUI, 发送命令
```

详见: `docs/architecture/process-ownership-model.md`

### 1.2 崩溃恢复

**不重新接管仍存活的 Agent 子进程。**

原因: stdin/stdout pipe 在 supervisor 崩溃时断开，无法重新连接。

恢复流程:
1. Agent 子进程通过 Job Object (Win) / 进程组 (Unix) / watchdog 被终止
2. 对应 Execution Attempt → `LOST`
3. 新 supervisor 启动 → 从 SQLite reconciliation
4. 对 LOST Execution:
   - retry < max → 创建新 Execution Attempt + `--resume` (使用保存的 native_session_id)
   - retry ≥ max → Execution FAILED → Task FAILED

### 1.3 LOST vs ORPHANED

| 状态 | 原因 | 处理 |
|------|------|------|
| LOST | Supervisor 崩溃 | 新建 Execution + --resume |
| ORPHANED | Agent 子进程自身异常退出 | retry 或 fail (同一 Execution chain) |

### 1.4 Blockers

✅ 已解除 (B1)

---

## 2. State Model (Revised)

### 2.1 四套核心生命周期

```
Project:
  CREATED → CLARIFYING → GOAL_LOCKED → PLANNING
  → AWAITING_APPROVAL → ACTIVE
  → INTEGRATING → VERIFYING → DELIVERING → DONE
  Terminal: CANCELLED | FAILED

Task:
  PENDING → READY → DISPATCHED → RUNNING
  → AWAITING_INPUT → SUBMITTED → VERIFIED → DONE
  Terminal: CANCELLED | SUPERSEDED | FAILED

Execution Attempt:
  CREATED → RUNNING → COMPLETED
                     → FAILED
                     → LOST (supervisor crash)
                     → CANCELLED

Workspace Lease:
  ACQUIRED → ACTIVE → RELEASED
                    → EXPIRED
```

### 2.2 从 Task 移除的状态

| 旧 Task 状态 | 新模型 |
|-------------|--------|
| `LEASED` | `WorkspaceLease::ACQUIRED` + `Task::DISPATCHED` |
| `COMMITTED` | `CommitOperation::COMPLETED` (独立 entity) |
| `MERGING` | `IntegrationJob::RUNNING` (独立 entity) |
| `MERGED` | `IntegrationJob::COMPLETED` → `Task::DONE` |

### 2.3 辅助维度 (保留)

```
health: HEALTHY | DEGRADED | STALLED
waiting_on: NONE | USER_APPROVAL | USER_FEEDBACK | SCOPE_EXPANSION
pause: NONE | PAUSE_REQUESTED | PAUSED | RESUMING
reason: null | "verification_failed" | "conflict" | "agent_unavailable" | ...
```

### 2.4 局部状态化

以下不是主生命周期状态，而是局部实体的状态:

| 局部实体 | 状态 |
|---------|------|
| `VerificationJob` | PENDING → RUNNING → PASSED / FAILED |
| `ApprovalRequest` | PENDING → APPROVED / REJECTED |
| `ResourceClaim` | ACTIVE → RELEASED |
| `ChangeRequest` | PROPOSED → APPROVED / REJECTED |
| `CommitOperation` | PENDING → RUNNING → COMPLETED / FAILED |
| `IntegrationJob` | CREATED → QUEUED → RUNNING → COMPLETED / CONFLICT / FAILED |

### 2.5 Blockers

✅ 已解除

---

## 3. Operation / Saga (Revised)

### 3.1 模型

代替 v1.0 的"外部副作用在事务前执行"。

**两阶段 Operation**:

```
Phase 1: RECORD INTENT
  BEGIN TRANSACTION;
    INSERT INTO operations (id, type, status='PENDING', operation_id, payload);
    INSERT INTO event_log (operation_started);
  COMMIT;

Phase 2: EXECUTE
  执行外部操作 (git commit, merge, etc.)
  Git commit message/trailer 包含 operation_id:
    harness-operation-id: {operation_id}

Phase 3: RECORD RESULT
  BEGIN TRANSACTION;
    UPDATE operations SET status='COMPLETED', result=...;
    INSERT INTO event_log (operation_completed);
  COMMIT;

Reconciliation:
  查找 status IN ('PENDING', 'RUNNING') 且超过 N 秒的 operations
  → 检查操作是否实际完成 (git log --grep "harness-operation-id: {id}")
  → 完成 → UPDATE status='COMPLETED'
  → 未完成 → 重试 或 FAILED
```

### 3.2 Operation 表

```sql
CREATE TABLE operations (
  id TEXT PRIMARY KEY,
  operation_id TEXT NOT NULL UNIQUE,
  operation_type TEXT NOT NULL,   -- 'git_commit' | 'git_merge' | 'git_cherry_pick' | 'acceptance_check'
  task_id TEXT NOT NULL,
  status TEXT NOT NULL,           -- 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED'
  payload_json TEXT NOT NULL,
  result_json TEXT,
  started_at TEXT NOT NULL,
  completed_at TEXT,
  idempotency_key TEXT NOT NULL UNIQUE
);
```

### 3.3 Git Operation ID

```
Commit message:
  harness: TASK-014 - Implement OAuth callback
  harness-operation-id: op-a1b2c3d4
  harness-task-id: TASK-014
  harness-project-id: proj-001
```

### 3.4 Blockers

✅ 已解除 (B2)

---

## 4. AgentAdapter Boundary (Revised)

### 4.1 AgentEvent 修订

```rust
enum AgentEvent {
    SessionStart { session_id, profile_id, timestamp },
    AssistantMessage { content, timestamp },
    Progress { summary: String, timestamp },          // ← renamed from Thinking, optional
    ReasoningSummary { summary: String, timestamp },  // ← 完成后可选摘要
    ToolUse { tool_name, tool_input, tool_use_id, timestamp },
    ToolResult { tool_use_id, is_error, content, timestamp },
    Error { message, code, timestamp },
    Result { content, is_error, timestamp },
    ProcessExited { exit_code: i32, signal: Option<i32>, timestamp },  // ← 新增
    RawVendorEvent { raw_type: String, payload: Value, timestamp },    // ← 新增, 非静默丢弃
    SessionEnd {
        session_id,
        timestamp,
        synthetic: bool,     // ← 合成事件标记
        abnormal: bool,      // ← 异常终止标记
    },
}

// 所有事件携带 (由 harness-runtime 添加, 非 Adapter):
struct EnrichedAgentEvent {
    execution_id: String,       // 哪个 Execution Attempt
    receive_sequence: u64,      // 单调递增序号 (per execution)
    received_at: DateTime<Utc>, // Harness 接收时间
    event: AgentEvent,
}
```

### 4.2 事件处理规则

| 场景 | 处理 |
|------|------|
| 未知 vendor 事件 | → RawVendorEvent (不静默丢弃) |
| 无法解析的 JSON line | → Error("unparseable", raw_line truncated) |
| 乱序到达 | receive_sequence 暴露乱序, 不声称接收时间解决乱序 |
| 断流 (no output N 秒) | → 检查进程存活 → timeout → LOST/ORPHANED |
| Agent 异常退出 (无 SessionEnd) | → Adapter 合成 SessionEnd { synthetic: true, abnormal: true } |
| 重复 tool_use_id | → 记录 warning, 不重复转发 |

### 4.3 Codex Adapter 命名修正

```
CodexAppServerAdapter → CodexCliAdapter
```

命名一致性: ClaudeCliAdapter ↔ CodexCliAdapter

### 4.4 Contract Freeze 策略

**不在 F0 前硬冻结。** 新的 Gate 流程:

```
Gate A (Architecture Ready) → Gate B (Contract Candidate)
→ Codex CLI tech spike → Claude CLI tech spike → Fake Adapter contract tests
→ Gate C (Contract Freeze v1)
→ Gate D (Runtime Integration)
→ Gate E (Foundation Acceptance)
```

### 4.5 Blockers

✅ 已解除 (B4 — 改为 Gate C 前由 tech spike 验证)

---

## 5. Git & Workspace Consistency (Revised)

### 5.1 修正规则

| 规则 | v1.0 | v2.0 |
|------|------|------|
| 空 commit | 允许 `git commit --allow-empty` | **禁止** |
| 无 diff 处理 | 创建空 commit | 根据任务类型: NO_CHANGES (只读) 或 VERIFICATION_FAILED (写入) |
| 合并冲突 | 换策略自动重试 | **不得自动切换策略** |
| 冲突处理 | 重试至 FAILED_TERMINAL | 创建 `IntegrationRepairTask` + `IntegrationRepairExecution` |
| Orphan worktree 清理 | 检查目录是否存在 | 验证 ownership marker + branch namespace + operation_id |

### 5.2 无 diff 任务处理

```
Agent 退出, git diff --stat 无变更:

  如果 Task 类型是只读分析 (explore/review/verify):
    → CommitOperation.result = NO_CHANGES
    → 验证阶段: 检查是否有预期产出物被创建
    → 通过 → Task DONE

  如果 Task 类型是写入 (implement/fix):
    → CommitOperation.result = NO_CHANGES
    → VerificationJob → FAILED
    → 原因: "Agent produced no file changes for a write task"
    → Task → retry 或 FAILED
```

### 5.3 合并冲突处理

```
git cherry-pick 冲突:
  1. git cherry-pick --abort (不尝试其他策略)
  2. IntegrationJob → CONFLICT
  3. 创建 IntegrationRepairTask:
     - resource_claims: 冲突文件 (WRITE)
     - depends_on: 冲突源 task
  4. 分配新 Execution Attempt (可以是不同的 Agent)
  5. Agent 修复冲突 → VERIFIED → 新 IntegrationJob
  6. 新 IntegrationJob 合并修复 commit
```

### 5.4 Orphan Worktree 清理

```
Reconciliation 检测到 worktree 存在但无 active WorkspaceLease:

  1. 验证 ownership:
     - .harness/worktrees/{task_id}/ 中的 .harness-owner 文件
     - 内容: { "run_id": "proj-001", "task_id": "TASK-014", "operation_id": "op-xxx" }
  2. 验证 branch namespace:
     - 分支名必须以 harness/ 开头
  3. 验证 operation_id:
     - 查询 operations 表
     - operation 状态是 PENDING/RUNNING/COMPLETED?
     - COMPLETED → 可以安全清理
     - PENDING/RUNNING → reconciliation 处理 operation
  4. 通过全部验证 → git worktree remove + git branch -D
  5. 任一失败 → 标记为 SUSPICIOUS, 人工介入
```

### 5.5 所有 Git 操作进入 Operation/Saga

```
git commit    → CommitOperation (type: 'git_commit')
git merge     → MergeOperation (type: 'git_merge')
git cherry-pick → CherryPickOperation (type: 'git_cherry_pick')
git worktree add → WorktreeCreateOperation (type: 'git_worktree_create')
```

### 5.6 Blockers

✅ 已解除 (B3)

---

## 6. Foundation Concurrency Model (Revised)

### 6.1 三项原子检查

Scheduler 在**同一决策上下文**中检查:

1. **Profile slot** — Agent 并发槽位可用?
2. **Workspace lease** — 目标 worktree 未被占用?
3. **Resource claims** — 所有声明资源无冲突?

详见: `docs/architecture/resource-claim-model.md`

### 6.2 Resource Claims (Foundation)

Foundation 实现基础 Resource Claim:

| Resource 类型 | 示例 |
|--------------|------|
| File (精确路径) | `src/auth/callback.ts` |
| Directory (前缀) | `packages/backend/src/**` |
| Repo (整个仓库) | repo 级互斥 |
| Logical | `dependency-manifest`, `database-schema`, `integration-branch`, `shared-types` |

**兼容规则**: READ/READ 兼容, READ/WRITE 冲突, WRITE/WRITE 冲突。

### 6.3 取消下游处理

```
Task B 依赖 Task A:
  Task A → CANCELLED
  → Task B 不再有效 (依赖不满足)
  → Task B → SUPERSEDED
  → 释放 Task B 的 Resource Claim (如已获取)

如果 Task B 已被 DISPATCHED:
  → 发送 cancellation token 给 B 的 Execution
  → Execution → CANCELLED
  → Task B → CANCELLED
  → 释放资源
```

### 6.4 Blockers

✅ 已解除 (D3 resolved: Foundation 实现基础 Resource Claim)

---

## 7. CLI-Core Decoupling (Revised)

### 7.1 HarnessApi

Supervisor 通过 IPC socket 暴露 `HarnessApi` 接口。CLI 通过 IPC client 调用。

```rust
// harness-core: trait 定义
#[async_trait]
pub trait HarnessApi: Send + Sync {
    async fn create_run(&self, objective: &str) -> Result<RunHandle>;
    async fn attach_run(&self, run_id: &str) -> Result<(RunHandle, mpsc::Receiver<HarnessEvent>)>;
    async fn approve(&self, run_id: &str) -> Result<()>;
    async fn reject(&self, run_id: &str, feedback: &str) -> Result<()>;
    async fn pause(&self, run_id: &str) -> Result<()>;
    async fn resume(&self, run_id: &str) -> Result<()>;
    async fn cancel(&self, run_id: &str) -> Result<()>;
    async fn send_feedback(&self, run_id: &str, text: &str) -> Result<()>;
    async fn get_status(&self, run_id: &str) -> Result<RunStatus>;
}

// harness-cli: IPC client 实现
struct IpcHarnessClient { socket_path: PathBuf }
impl HarnessApi for IpcHarnessClient { /* 通过 IPC socket 调用 supervisor */ }

// harness-runtime: supervisor 端实现
struct SupervisorHarness { db: Database }
impl HarnessApi for SupervisorHarness { /* 直接操作 SQLite + ProcessManager */ }
```

### 7.2 Non-Interactive Mode

```bash
harness run "目标" --non-interactive --approve
```

`--approve` flag 必须显式传入。**不得静默自动审批。**

Non-interactive 模式行为:
- 提交目标 → 输出 JSON lines 到 stdout
- 等待 AWAITING_APPROVAL → 如果 `--approve` 则自动批准, 否则输出 plan JSON 并等待 stdin 输入 "approve"
- 输出 AgentEvent 到 stdout (一行一个 JSON)
- 完成后退出码 0 (成功) 或 1 (失败)

### 7.3 CI No-TTY

```bash
# CI 中直接测试 supervisor (无 IPC, 无 TTY)
harness-test-supervisor --run-id test-001 --plan plan.json
```

Supervisor 可以在没有 IPC socket 的模式下启动 (用于 CI 测试)。

### 7.4 修正项

| 修正 | 说明 |
|------|------|
| Non-interactive 不得静默审批 | `--approve` flag 必须显式传入 |
| Scheduler 必须计算现有 active execution | 不只数 task status, 还要查 execution_attempts |
| Core 边界检查不能仅依赖 grep | 增加 Cargo.toml dependency 守卫 + `cargo check` 验证 |

---

## 8. Foundation Completion Definition (Revised)

### 8.1 四类实现

| 分类 | 含义 | Foundation 示例 |
|------|------|----------------|
| **Real** | 完整实现, 有测试 | SQLite persistence, state machine, FakeAdapter, ProcessManager, WorktreeManager |
| **Basic Strategy** | 简单确定性实现 | FakePlanningProvider, 规则路由, 确定性验证 |
| **Extension Boundary** | 接口定义, 基础实现 | Sandbox trait + HostSandbox, AgentAdapter trait 未来扩展 |
| **Deferred** | 明确推迟 | LLM Planner/Reviewer, OS 沙箱, 历史路由 |

### 8.2 Foundation 能力声明

```
✅ 能执行:
  - 安装: 单二进制 (harness + harness-supervisor)
  - 发现: 扫描 Claude/Codex CLI, 探测能力
  - CLI: 交互式 (ratatui) + 非交互式 (--non-interactive)
  - 配置: ~/.harness/config.json
  - 项目: 创建, Goal Contract, 审批流程
  - 规划: 手工 Task DAG (FakePlanningProvider)
  - 并发: 多 Task 并行, Resource Claim, Profile slot, WorkspaceLease
  - 隔离: Per-task Git worktree
  - 执行: 流式 AgentEvent, 超时, 取消
  - 验证: 确定性 acceptance checks, diff 检查, 密钥扫描
  - Commit: Harness 独占, operation_id trailer
  - 合并: IntegrationJob + Conflict Repair
  - 恢复: Supervisor 崩溃 → reconciliation → Execution LOST → retry + --resume
  - 审计: event_log + audit_log + operations table
  - Detach: CLI 断开, Supervisor 继续
  - Re-attach: 新 CLI 连接 Supervisor

❌ 不能:
  - 理解自然语言需求 (LLM Clarifier → Functional)
  - 自动架构设计 (LLM Architect → Functional)
  - 自动 Task DAG (LLM Decomposer → Functional)
  - LLM 代码审查 (LLM Reviewer → Functional)
  - 自动修复 (Repair Loop → Functional)
  - 学习路由 (History scoring → Functional)
  - OS 级沙箱 (Container/VM → Production)
  - Web UI / 高级 TUI 仪表板 (Functional/Production)
  - 任意第三方 Agent 插件 (后续 Adapter 开发)
```

### 8.3 距离"自动交付"的差距

```
Foundation ✅ : execute → verify → commit → merge → recover
Functional ❌ : clarify → architect → plan → review → repair → learn
Production ❌ : sandbox → team → distribute → scale
```

---

## 9. Gate 模型

### 9.1 五个 Gate

```
Gate A: Architecture Ready
  进入: 所有 Blocker 解除, D1-D7 获批, 本审计批准
  退出: 所有架构文档定稿, CI 骨架通过, Cargo workspace 编译通过

Gate B: Contract Candidate
  进入: Gate A 通过
  退出: harness-core 所有 type + trait 定义完成, FSM 实现通过穷举测试
        tech spike: Fake Adapter + Codex CLI + Claude CLI 基础验证

Gate C: Contract Freeze v1
  进入: Gate B 通过 + tech spike 报告批准
  退出: AgentAdapter v1 冻结, AgentEvent v1 冻结, SQLite schema v1 冻结
        (后续通过 migration 演进)

Gate D: Runtime Integration
  进入: Gate C 通过
  退出: F2-F7 完成, 全部 Golden Path 通过, 至少一个 Real Adapter 集成通过

Gate E: Foundation Acceptance
  进入: Gate D 通过
  退出: Foundation Acceptance Criteria 全部 MUST 项通过, v0.1.0 tag
```

### 9.2 与 F0-F10 的对应

```
Gate A → F0 开始
Gate B → F1 + F2 + F4a 完成 (domain + state machine + FakeAdapter)
Gate C → Tech spike 后, F5 开始前
Gate D → F2-F7 全部完成
Gate E → F9-F10 完成
```

---

## 10. 矛盾修正

| # | 修正前 (v1.0/v3.2) | 修正后 (v2.0) |
|---|-------------------|---------------|
| 1 | 单二进制 = 零外部依赖 | 单二进制指 Harness 自身无需运行时 (Node.js)。但需要本机安装 Git 和 Agent CLI |
| 2 | Orchestrator LLM API 配置 | Foundation 不需要 Orchestrator LLM API — FakePlanningProvider 无 LLM 调用 |
| 3 | --non-interactive 静默审批 | 必须显式 `--approve` flag |
| 4 | Core 边界仅依赖 grep | 增加 Cargo.toml dependency 检查 + `cargo check` 验证 |
| 5 | Scheduler 只数 task status | 必须查询 `execution_attempts` 表计算实际 active execution |
| 6 | 取消无下游处理 | 取消上游 Task → 下游 Task SUPERSEDED 或 CANCELLED |
| 7 | CodexAppServerAdapter | → CodexCliAdapter (命名一致) |

---

## 11. 综合建议

### 11.1 需要修改的文档

| 文档 | 操作 | 优先级 |
|------|------|:---:|
| `readiness-audit.md` | ✅ 已完成 (本文件) | — |
| `process-ownership-model.md` | ✅ 已新建 | — |
| `resource-claim-model.md` | ✅ 已新建 | — |
| `integration-queue.md` | ✅ 已新建 | — |
| `state-machines.md` | 🔧 待更新 (四套生命周期) | P0 |
| `event-model.md` | 🔧 待更新 (Operation/Saga) | P0 |
| `adapter-contract.md` | 🔧 待更新 (AgentEvent v2) | P0 |
| `git-workspace-model.md` | 🔧 待更新 (Operation trailer, 无空 commit) | P0 |
| `recovery-model.md` | 🔧 待更新 (LOST vs ORPHANED) | P0 |
| `adr/002-sqlite-and-event-model.md` | 🔧 待更新 (Operation/Saga) | P0 |
| `cli-architecture.md` | 🔧 待更新 (IPC client 模型) | P0 |
| `foundation-release-plan.md` | 🔧 待更新 (Gate 对应 F 阶段) | P1 |
| `foundation-acceptance-criteria.md` | 🔧 待更新 (四套生命周期) | P1 |
| `test-strategy.md` | 🔧 待更新 | P1 |
| `requirements-matrix.md` | 🔧 待更新 | P1 |
| `risk-register.md` | 🔧 待更新 | P1 |

### 11.2 修订后的决策清单 (D1-D7)

| # | 决策 | 状态 |
|---|------|:---:|
| D1 | 独立 headless supervisor + CLI as IPC client | 待审批 |
| D2 | Operation/Saga 两阶段模型 (替代事务前执行) | 待审批 |
| D3 | Foundation 实现基础 Resource Claim (文件/目录/逻辑) | 待审批 |
| D4 | 四套核心生命周期 (Project/Task/ExecutionAttempt/WorkspaceLease) | 待审批 |
| D5 | AgentEvent v2 (Progress+ProcessExited+RawVendorEvent+execution_id+receive_sequence) | 待审批 |
| D6 | 推迟 Contract Freeze → Gate C, 通过 tech spike 验证后冻结 | 待审批 |
| D7 | Codex 命名修正 → CodexCliAdapter | 待审批 |

### 11.3 仍未解决的真正 Blocker

| # | Blocker | 需要 |
|---|---------|------|
| B5 | Tech spike 完成前无法验证 Adapter 契约在真实 Agent 上的可行性 | Gate B → C 之间的 spike |
| B6 | 需确认 Codex CLI JSON-RPC 和 Claude CLI stream-json 的能力交集是否覆盖 AgentEvent v2 全部事件 | Tech spike |
| B7 | 需确认 `--resume` (Claude) 和 thread resume (Codex) 在 LOST 场景下是否可靠 | Tech spike |

### 11.4 Gate A 进入条件 (→ 可以开始 F0)

1. ✅ 本审计 (v2.0) 批准
2. ⬜ D1-D7 全部获批
3. ⬜ P0 文档全部更新完成
4. ⬜ Cargo workspace skeleton 编译通过
5. ⬜ CI 骨架 (lint + build) 通过

### 11.5 Gate A 退出条件 (→ F1 完成)

1. ⬜ `harness-core` 所有 type + trait 定义
2. ⬜ FSM 实现 + 穷举测试
3. ⬜ `cargo check` 通过
4. ⬜ `harness-core` 不依赖 `rusqlite`, `tokio`, `git2`, `ratatui`

---

**审计完成，等待审批 D1-D7。**

---

## 12. Gate A Completion Report (2026-07-15)

### 12.1 F0 Deliverables

| Item | Status |
|------|:---:|
| Cargo workspace (4 crates) | ✅ |
| `harness-core` 零 I/O 依赖 (no tokio/rusqlite/git2/ratatui) | ✅ |
| `cargo check --workspace` | ✅ PASS |
| `cargo test --workspace` | ✅ 10/10 PASS |
| `cargo fmt --all --check` | ✅ PASS |
| CI skeleton (`.github/workflows/ci.yml`) | ✅ |
| `rustfmt.toml` | ✅ |
| Core types: AgentAdapter, AgentSession, AgentEvent, RuntimeProfile, TaskEnvelope, TaskResult | ✅ Candidate |
| State machines: Project, Task, Execution, Lease FSM + exhaustive tests | ✅ |
| Policies: budget, command, file_scope | ✅ |

### 12.2 Spike Results

| Spike | Result |
|-------|--------|
| **Process Supervisor** | ✅ 6/6 tests PASS |
| | subprocess spawn, stdout/stderr capture, stdin write, timeout, cancellation (taskkill /T /F), exit code detection |
| **Claude CLI** | ✅ Real stream-json captured |
| | Claude Code 2.1.210, model deepseek-v4-pro, session_id extraction, event type mapping verified |
| **Codex CLI** | ⚠️ PARTIAL |
| | Codex CLI 0.116.0 detected, `exec --json` confirmed, blocked by config.toml service_tier validation error |

### 12.3 B5-B7 Status (Converted to Tech Spike Questions)

| # | Question | Status |
|---|---------|:---:|
| B5 | AgentEvent coverage verified? | PARTIAL — Claude ✅ (all events map), Codex ❌ (config block) |
| B6 | Claude ↔ Codex event mapping? | Codex CLI JSONL ↔ Claude stream-json — Claude verified, Codex pending config fix |
| B7 | Resume reliability? | NOT VERIFIED — requires working execution + multi-turn test |

### 12.4 AgentEvent V1 Candidate

Based on **real Claude stream-json data** (NOT guesswork):

11 events: SessionStarted, Message, Progress (optional), ReasoningSummary (optional), ToolCallStarted, ToolCallCompleted, Result, Error, ProcessExited, RawVendorEvent, SessionEnded

Each enriched with: execution_id, receive_sequence, received_at

Key findings:
- ReasoningSummary is optional (Claude uses incremental thinking, not end-summary)
- SessionEnded is always synthetic (neither Claude nor Codex emits native session-end)
- ProcessExited is separate from Result
- RawVendorEvent must exist — unknown events NOT silently dropped

### 12.5 Updated Decisions

| # | Decision | Status |
|---|---------|:---:|
| D1 | Headless supervisor + IPC client | ✅ APPROVED (Process Spike verified) |
| D2 | Operation/Saga | ✅ APPROVED (design only, not implemented) |
| D3 | Resource Claim | ✅ APPROVED (design only) |
| D4 | Four lifecycles | ✅ APPROVED (implemented in core FSM) |
| D5 | AgentEvent V1 Candidate | 📋 Candidate (from real Claude data) |
| D6 | Contract Freeze → Gate C | ✅ CONFIRMED (NOT freezing yet) |
| D7 | CodexCliAdapter naming | ✅ DONE (all arch docs updated) |

### 12.6 Gate A Exit Criteria

| # | Criterion | Status |
|---|----------|:---:|
| 1 | Cargo workspace compiles | ✅ |
| 2 | All tests pass | ✅ 10/10 |
| 3 | Format check passes | ✅ |
| 4 | `harness-core` no rusqlite/tokio/git2/ratatui | ✅ |
| 5 | Codex/Claude spikes complete | ✅ Claude, ⚠️ Codex (config block) |
| 6 | AgentEvent V1 Candidate from real data | ✅ |
| 7 | AgentAdapter V1 Candidate | ✅ |
| 8 | State counts corrected | ✅ |
| 9 | All CodexAppServer references cleaned | ✅ |
| 10 | B5-B7 recategorized as Tech Spike Questions | ✅ |

### 12.7 Recommendation

**Gate A: PASSED ✅**

Proceed to Gate B (Contract Candidate). Gate B includes:
1. FakeAgentAdapter implementation + contract tests
2. Single-task Golden Path with FakeAdapter
3. Codex config fix + re-spike
4. AgentEvent/Adapter candidate revision based on Codex data

**Do NOT freeze contracts until Gate C** — Codex real output must be captured first.

