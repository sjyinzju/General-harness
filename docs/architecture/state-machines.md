# State Machines — Agent Harness

> **文档类型**: 架构规范
> **版本**: v1.0
> **日期**: 2026-07-14

---

## 1. 设计原则

1. **LLM 可以建议，状态变更由确定性规则执行**
2. **业务代码不能直接修改状态字段**，只能通过 `TransitionService`
3. **所有转换通过幂等的 command → event → projection 流程**
4. **状态不变量在每个转换前后必须保持**

---

## 2. 项目级状态机（Project FSM）

### 2.1 状态定义

| 状态 | 含义 | 是否为终端状态 |
|------|------|:---:|
| `CREATED` | 项目已创建，等待需求输入 | ❌ |
| `CLARIFYING` | 正在与用户对话澄清需求 | ❌ |
| `GOAL_LOCKED` | Goal Contract 已确认，不可随意修改 | ❌ |
| `PLANNING` | 正在生成架构设计和任务 DAG | ❌ |
| `AWAITING_APPROVAL` | 计划已生成，等待用户审批 | ❌ |
| `SCHEDULING` | 用户已批准，正在调度第一个任务 | ❌ |
| `RUNNING` | 任务正在执行中 | ❌ |
| `PAUSING` | 正在暂停（等待正在运行的任务完成） | ❌ |
| `PAUSED` | 已暂停，所有任务已停止 | ❌ |
| `RESUMING` | 正在恢复暂停的任务 | ❌ |
| `RECOVERING` | 崩溃后正在恢复状态 | ❌ |
| `INTEGRATING` | 所有任务完成，正在合并 worktree | ❌ |
| `VERIFYING` | 正在执行最终端到端验收 | ❌ |
| `REPAIRING` | 验收失败，正在修复 | ❌ |
| `DEGRADED` | 部分 Agent 不可用，降级运行中 | ❌ |
| `BLOCKED` | 遇到无法自动解决的问题 | ❌ |
| `CANCELLING` | 正在取消（清理资源中） | ❌ |
| `CANCELLED` | 已取消 | ✅ |
| `FAILED` | 项目失败（无法自动恢复） | ✅ |
| `DELIVERING` | 正在生成交付物 | ❌ |
| `DONE` | 成功完成 | ✅ |

### 2.2 合法状态转换表

| 从 | 到 | 前置条件 | Actor |
|----|----|-----------|-------|
| CREATED | CLARIFYING | Project 已保存 | system |
| CLARIFYING | GOAL_LOCKED | Goal Contract v1 已创建且内容完整 | orchestrator |
| GOAL_LOCKED | PLANNING | 至少一个 AVAILABLE Runtime Profile | orchestrator |
| PLANNING | AWAITING_APPROVAL | Plan v1 已生成，包含至少一个 Task | orchestrator |
| AWAITING_APPROVAL | SCHEDULING | 用户输入了 "approve" | user |
| AWAITING_APPROVAL | PLANNING | 用户提供了修改意见 | user |
| AWAITING_APPROVAL | CANCELLING | 用户输入了 "cancel" | user |
| SCHEDULING | RUNNING | 第一个任务已 LEASED | system |
| RUNNING | PAUSING | 用户请求暂停 | user |
| RUNNING | INTEGRATING | DAG 中所有非 CANCELLED 任务都 MERGED | system |
| RUNNING | DEGRADED | 部分 Runtime Profile 变为 UNAVAILABLE | system |
| RUNNING | BLOCKED | 有 FAILED_TERMINAL 任务且无法重规划 | orchestrator |
| PAUSING | PAUSED | 所有 RUNNING 任务已停止或达到安全点 | system |
| PAUSED | RESUMING | 用户请求恢复 | user |
| RESUMING | RUNNING | 所有暂停的任务已重新 LEASED | system |
| DEGRADED | RUNNING | 替代 Runtime Profile 已找到并接管 | system |
| DEGRADED | BLOCKED | 无可用替代方案 | system |
| INTEGRATING | VERIFYING | 所有 merge 操作成功完成 | system |
| INTEGRATING | REPAIRING | merge 冲突无法自动解决 | system |
| VERIFYING | DELIVERING | 所有 acceptance checks 通过 | system |
| VERIFYING | REPAIRING | 有 acceptance check 失败 | system |
| REPAIRING | INTEGRATING | 修复后重新集成 | orchestrator |
| REPAIRING | BLOCKED | 修复失败次数超限 | system |
| DELIVERING | DONE | 交付物已生成 | system |
| CANCELLING | CANCELLED | 所有子进程已终止、worktree 已清理 | system |
| 任意非终端 | FAILED | 不可恢复的系统错误 | system |
| 任意非终端 | CANCELLING | 用户请求取消 | user |

### 2.3 非法转换示例

```
❌ CREATED → RUNNING        (跳过了所有中间状态)
❌ PLANNING → DONE          (未执行)
❌ PAUSED → DONE            (未恢复和验证)
❌ DONE → RUNNING           (终端状态不可逆)
❌ FAILED → RUNNING         (需要新建 Project)
❌ CANCELLED → 任何状态      (终端状态不可逆)
```

### 2.4 状态不变量

```
RUNNING:     至少有 1 个 task 状态为 RUNNING/LEASED/VERIFYING/COMMITTING
PAUSED:      所有 task 状态为 READY/PENDING/CANCELLED/FAILED_TERMINAL（无不安全的中断状态）
INTEGRATING: 所有 task 状态为 MERGED 或 CANCELLED
DONE:        所有非 CANCELLED task 状态为 MERGED
DEGRADED:    至少 1 个 AVAILABLE profile 变为 UNAVAILABLE/DEGRADED
```

---

## 3. 任务级状态机（Task FSM）

### 3.1 状态定义

| 状态 | 含义 | 是否为终端状态 |
|------|------|:---:|
| `PENDING` | 任务已定义，依赖未满足 | ❌ |
| `READY` | 依赖已满足，等待调度 | ❌ |
| `LEASED` | 已分配 Runtime Profile + WorkspaceLease，等待 Agent 启动 | ❌ |
| `RUNNING` | Agent 子进程正在执行 | ❌ |
| `AWAITING_INPUT` | Agent 请求额外输入（如 scope 扩展） | ❌ |
| `SUBMITTED` | Agent 已完成，返回 TaskResult（声明） | ❌ |
| `VERIFYING` | Harness 正在执行验收检查 | ❌ |
| `VERIFIED` | 所有验收检查通过 | ❌ |
| `COMMITTED` | Harness 已创建 git commit | ❌ |
| `MERGING` | Harness 正在将 commit 合并到 integration 分支 | ❌ |
| `MERGED` | 已成功合并 | ❌ (功能性终端) |
| `FAILED_RETRYABLE` | 验证失败，可重试 | ❌ |
| `FAILED_TERMINAL` | 验证失败，已达最大重试次数 | ❌ |
| `BLOCKED` | 等待外部条件（用户输入/其他任务） | ❌ |
| `ORPHANED` | 进程崩溃，任务无 owner | ❌ |
| `SUPERSEDED` | 被新任务取代 | ✅ |
| `CANCELLED` | 已取消 | ✅ |

### 3.2 合法状态转换表

| 从 | 到 | 前置条件 | Actor |
|----|----|-----------|-------|
| PENDING | READY | 所有依赖任务的 status === MERGED | system |
| PENDING | SUPERSEDED | Change Request 导致此任务不再需要 | orchestrator |
| PENDING | CANCELLED | 用户或 Orchestrator 取消 | user/orchestrator |
| READY | LEASED | 有 AVAILABLE Runtime Profile + WorkspaceLease 获取成功 | system |
| READY | CANCELLED | 用户取消 | user |
| LEASED | RUNNING | AgentSession 已创建，TaskEnvelope 已发送 | system |
| LEASED | ORPHANED | 进程崩溃，无法确认 Agent 状态 | system (reconciliation) |
| LEASED | FAILED_RETRYABLE | Agent 启动失败 | system |
| RUNNING | AWAITING_INPUT | Agent 发出 scope expansion 等请求 | agent |
| AWAITING_INPUT | RUNNING | Orchestrator 批准请求 | orchestrator |
| AWAITING_INPUT | BLOCKED | Orchestrator 拒绝请求 | orchestrator |
| RUNNING | SUBMITTED | Agent 正常结束，返回 TaskResult | agent |
| RUNNING | ORPHANED | 进程崩溃 | system (reconciliation) |
| RUNNING | FAILED_RETRYABLE | Agent 异常退出 | system |
| SUBMITTED | VERIFYING | TransitionService 触发验证 | system |
| VERIFYING | VERIFIED | 所有 acceptanceChecks exitCode === 0 AND diff 在 allowedPaths 内 AND 密钥扫描通过 | system |
| VERIFYING | FAILED_RETRYABLE | 有 acceptanceCheck 失败 | system |
| VERIFIED | COMMITTED | Harness 成功执行 git add + commit | system |
| COMMITTED | MERGING | 无冲突 | system |
| MERGING | MERGED | 合并成功 | system |
| MERGING | FAILED_RETRYABLE | 合并冲突，需重新处理 | system |
| FAILED_RETRYABLE | RUNNING | retryCount < maxRetries | system |
| FAILED_RETRYABLE | FAILED_TERMINAL | retryCount >= maxRetries | system |
| FAILED_TERMINAL | BLOCKED | Orchestrator 决定等待 | orchestrator |
| FAILED_TERMINAL | READY | Orchestrator 决定换 Agent 重试 | orchestrator |
| ORPHANED | READY | Reconciliation: retryCount < maxRetries | system |
| ORPHANED | FAILED_TERMINAL | Reconciliation: retryCount >= maxRetries | system |
| BLOCKED | READY | 阻塞条件解除 | system/orchestrator |
| BLOCKED | CANCELLED | 用户决定放弃 | user |
| READY/LEASED/RUNNING | SUPERSEDED | 被更高优先级的任务取代 | orchestrator |

### 3.3 非法转换示例

```
❌ PENDING → RUNNING        (跳过 READY 和 LEASED)
❌ RUNNING → VERIFIED       (跳过 SUBMITTED 和 VERIFYING)
❌ VERIFIED → FAILED_RETRYABLE (验证已通过，不能回退)
❌ MERGED → 任何状态         (功能性终端)
❌ SUPERSEDED → 任何状态     (终端)
❌ CANCELLED → 任何状态      (终端)
```

### 3.4 状态不变量

```
READY:    所有依赖任务都是 MERGED
LEASED:   有 WorkspaceLease，在 SQLite 中有 lease 记录
RUNNING:  有 AgentSession，子进程存活
SUBMITTED:TaskResult 非空，changedFiles 已收集
VERIFIED: VerificationEvidence 非空且全部 pass
COMMITTED:commitHash 非空
MERGED:   分支已合并到 integration
```

---

## 4. TransitionService 实现规范

### 4.1 接口

```typescript
interface TransitionService {
  transitionProject(
    projectId: string,
    to: ProjectStatus,
    context: TransitionContext
  ): Promise<StateTransitionResult>;

  transitionTask(
    taskId: string,
    to: TaskStatus,
    context: TransitionContext
  ): Promise<StateTransitionResult>;
}

interface TransitionContext {
  actor: "harness" | "orchestrator" | "user" | "system" | "agent";
  reason: string;
  evidence?: VerificationEvidence[];
  idempotencyKey: string;
}

interface StateTransitionResult {
  success: true;
  from: ProjectStatus | TaskStatus;
  to: ProjectStatus | TaskStatus;
  timestamp: string;
  eventId: string;
}
```

### 4.2 处理流程

```
1. 校验 idempotencyKey 是否已处理 → 是 → 返回已有结果
2. 获取当前状态
3. 查表：from → to 是否为合法转换 → 否 → 抛 TransitionError
4. 检查前置条件 → 不满足 → 抛 TransitionError (附缺失条件)
5. 在事务中：
   a. 生成 DomainEvent
   b. 写入 event_log
   c. 更新 projection
   d. 记录 idempotencyKey
6. 返回 StateTransitionResult
```

### 4.3 非法转换处理

```typescript
class TransitionError extends Error {
  constructor(
    public readonly currentStatus: string,
    public readonly targetStatus: string,
    public readonly reason: string,
    public readonly missingConditions?: string[]
  ) {
    super(`Illegal transition: ${currentStatus} → ${targetStatus}: ${reason}`);
  }
}
```

调用方不应吞掉 TransitionError —— 它表示编程错误或数据损坏。

### 4.4 幂等保证

- 每个 `idempotencyKey` 只能完整执行一次
- 重复调用返回缓存的结果（相同 `from → to + timestamp + eventId`）
- `idempotencyKey` 在 `event_log` 表中有 UNIQUE 约束

---

## 5. 崩溃恢复 (Reconciliation)

### 5.1 触发条件

- Harness 进程启动时
- 检测到子进程异常退出且未正常 finish

### 5.2 恢复流程

```
1. 查询所有非终端状态的 Project
2. 对每个 Project：
   a. 查询所有非终端状态的 Task
   b. 对每个 LEASED / RUNNING Task：
      - 检查是否有对应子进程存活
      - 存活 → 不处理（可能被其他 Harness 实例管理）
      - 不存活 → 标记为 ORPHANED
   c. 对每个 ORPHANED Task：
      - retryCount < maxRetries → 重置为 READY
      - retryCount >= maxRetries → 标记为 FAILED_TERMINAL
   d. 对 VERIFYING / COMMITTING / MERGING Task：
      - 有中间产物 → 尝试从中断点继续
      - 无中间产物 → 回退到 SUBMITTED
   e. 更新 Project 状态
```

### 5.3 孤儿进程清理

```
启动时执行：
1. 查询所有已知的 Agent 子进程 PID（从 process_executions 表）
2. 对每个 PID：检查进程是否存活
3. 存活：确认是否属于当前 Harness 实例管理的 task
4. 不属于 → 发送 SIGTERM → 超时 5s → SIGKILL
5. 记录清理日志
```
