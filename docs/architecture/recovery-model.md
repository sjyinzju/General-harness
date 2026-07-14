# Recovery Model — Agent Harness

> **文档类型**: 架构规范
> **版本**: v1.0
> **日期**: 2026-07-14

---

## 1. 恢复模型概述

Agent Harness 必须在进程崩溃、系统重启、Agent 子进程异常退出等场景下保证状态一致性。恢复分为三个层次：

1. **进程内恢复**：Agent 异常退出，Harness 仍在运行
2. **进程崩溃恢复**：Harness 自身崩溃或被杀
3. **系统重启恢复**：操作系统重启后

Foundation Release 实现层次 1 和 2。层次 3 是层次 2 的自然延伸（从 SQLite 恢复）。

---

## 2. 数据恢复保证

### 2.1 从不丢失的数据

- **event_log**：SQLite 中的 append-only 事件，WAL 模式保证 crash-safe
- **audit_log**：与 event_log 在同一事务中写入
- **已保存的 commit hash**：写入 event_log 前已在 Git 中创建

### 2.2 可能丢失的数据

- **Agent Stream Event**：未及时持久化的 Agent 输出（丢失最后几秒）
- **agent_events 表**：与 event_log 不是同一事务（性能考虑）
- **文件系统日志**：Agent transcript 的临时文件

### 2.3 可以完全重建的数据

- **projections**：从 event_log 完全重建
- **agent_events**：从 Agent transcript 日志文件重建（如有）

---

## 3. Reconciliation 流程

### 3.1 触发条件

- Harness 进程启动
- `harness resume` 命令
- 检测到子进程异常退出（SIGCHLD / process.on('exit')）

### 3.2 恢复步骤

```typescript
async function reconcile(state: AppState): Promise<ReconciliationReport> {
  const report: ReconciliationReport = { orphanedTasks: [], recoveredTasks: [], failedTasks: [] };

  // 1. 查询所有非终端状态的 Project
  const activeProjects = await projectionRepo.getNonTerminalProjects();

  for (const project of activeProjects) {
    // 2. 查询该 Project 下所有非终端状态的 Task
    const activeTasks = await projectionRepo.getNonTerminalTasks(project.id);

    for (const task of activeTasks) {
      switch (task.status) {
        case "LEASED":
        case "RUNNING": {
          // 3. 检查子进程是否仍存活
          const isAlive = await processManager.isProcessAlive(task.lastKnownPid);
          if (!isAlive) {
            await transitionService.transitionTask(task.id, "ORPHANED", {
              actor: "system",
              reason: `Process ${task.lastKnownPid} not found during reconciliation`,
              idempotencyKey: `reconcile-orphan-${task.id}-${Date.now()}`
            });

            // 4. 决定是否重试
            if (task.retryCount < task.maxRetries) {
              await transitionService.transitionTask(task.id, "READY", {
                actor: "system",
                reason: "Orphaned task returned to READY for retry",
                idempotencyKey: `reconcile-retry-${task.id}-${Date.now()}`
              });
              report.recoveredTasks.push(task.id);
            } else {
              await transitionService.transitionTask(task.id, "FAILED_TERMINAL", {
                actor: "system",
                reason: `Max retries (${task.maxRetries}) exhausted`,
                idempotencyKey: `reconcile-terminal-${task.id}-${Date.now()}`
              });
              report.failedTasks.push(task.id);
            }
          }
          break;
        }

        case "VERIFYING":
        case "COMMITTING":
        case "MERGING": {
          // 5. 检查是否有部分完成的工作
          const checkpoint = await checkpointStore.getLatest(task.id);
          if (checkpoint) {
            // 从中断点继续
            await resumeFromCheckpoint(task, checkpoint);
            report.recoveredTasks.push(task.id);
          } else {
            // 回退到安全状态
            await transitionService.transitionTask(task.id, "SUBMITTED", {
              actor: "system",
              reason: "Interrupted during critical phase, rolling back to SUBMITTED",
              idempotencyKey: `reconcile-rollback-${task.id}-${Date.now()}`
            });
            report.recoveredTasks.push(task.id);
          }
          break;
        }
      }
    }

    // 6. 更新 Project 状态
    await reconcileProjectStatus(project.id);
  }

  return report;
}
```

---

## 4. Checkpoint 策略

### 4.1 检查点触发时机

- 任务状态变为 `VERIFIED` 时
- 任务状态变为 `COMMITTED` 时
- 任务状态变为 `MERGED` 时
- 每 N 分钟心跳（对长时间运行的 RUNNING 任务）
- 用户请求暂停时

### 4.2 检查点内容

```typescript
interface Checkpoint {
  id: string;
  projectId: string;
  taskId: string;
  createdAt: string;

  // 当前状态
  projectStatus: ProjectStatus;
  taskStatus: TaskStatus;

  // 关键数据引用
  commitHash?: string;
  verificationEvidenceRefs: string[];
  workspaceLeaseId?: string;

  // 恢复信息
  lastCompletedStep: string;      // 上一个成功完成的操作名
  nextStep: string;                // 下一个要执行的操作名
  resumeData: Record<string, unknown>;  // 步骤特定的恢复数据

  // Agent 状态（如适用）
  agentSessionId?: string;
  nativeSessionId?: string;

  // Git 状态
  branchName?: string;
  worktreePath?: string;
  baseCommitHash?: string;
}
```

### 4.3 检查点保存

```
.harness/checkpoints/{projectId}/{taskId}/
├── v001_verified.json
├── v002_committed.json
├── v003_merged.json
└── latest.json              # 指向最新版本的符号链接或副本
```

---

## 5. 进程管理器

### 5.1 子进程注册

```typescript
class ProcessManager {
  /** 启动 Agent 子进程 */
  async spawn(
    taskId: string,
    adapter: AgentAdapter,
    session: AgentSession,
    options: SpawnOptions
  ): Promise<ProcessHandle>;

  /** 检查进程是否存活 */
  async isProcessAlive(pid: number): Promise<boolean>;

  /** 通过 taskId 终止进程（先 SIGTERM，超时后 SIGKILL） */
  async terminate(taskId: string, gracefulTimeoutMs?: number): Promise<void>;

  /** 强制终止进程（SIGKILL，不做优雅清理） */
  async kill(pid: number): Promise<void>;

  /** 列出所有已知的活跃子进程 */
  listActiveProcesses(): ProcessInfo[];

  /** 终止所有已知的活跃子进程（用于 shutdown） */
  async killAll(timeoutMs?: number): Promise<void>;
}
```

### 5.2 子进程健康监控

- 每 N 秒检查进程是否存活（通过 pid + OS API）
- 如果进程已退出但任务状态仍为 RUNNING → 触发 reconciliation
- 超时 → 发送 SIGTERM → 等待 graceful timeout → SIGKILL → 触发 reconciliation

---

## 6. 幂等操作保证

### 6.1 需要幂等的操作

| 操作 | 幂等策略 |
|------|---------|
| Git worktree 创建 | 检查目录是否已存在且配置正确 |
| Git commit | 检查是否已存在相同内容的 commit |
| Git merge | 检查目标分支是否已包含源分支 |
| 状态转换 | idempotencyKey 去重 |
| 文件创建 | 检查文件是否已存在且内容一致 |
| Acceptance check 执行 | 检查是否已有相同参数的结果 |

### 6.2 Idempotency Key 格式

```
{operation}-{resource_id}-{timestamp_or_nonce}

示例:
  transition-task-TASK-014-20260714-001
  git-commit-TASK-014-a1b2c3d4
  verify-TASK-014-run1
```

---

## 7. 崩溃场景矩阵

| 崩溃时机 | 影响 | 恢复方式 |
|---------|------|---------|
| Agent 正在执行 (RUNNING) | 子进程退出，部分文件已修改 | reconciliation → ORPHANED → READY（重试） |
| Agent 刚返回结果 (SUBMITTED) | TaskResult 已保存 | 从 SUBMITTED 继续 → VERIFYING |
| 验收检查执行中 (VERIFYING) | 部分检查可能已执行 | 重新执行全部验收检查（幂等） |
| Git commit 执行中 (COMMITTING) | commit 可能已创建也可能未创建 | 检查是否存在 → 有则继续，无则重做 |
| Merge 执行中 (MERGING) | 合并可能部分完成 | 检查分支状态 → 恢复或回滚 |
| Harness 自身崩溃 | 所有状态在 SQLite | 启动时全量 reconciliation |
| 系统断电 | SQLite WAL 保证 event_log 完整 | 启动时重放 WAL → reconciliation |

---

## 8. 数据完整性

### 8.1 SQLite WAL 模式

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
```

WAL 模式保证：崩溃后数据库不损坏，已提交的事务不丢失。

### 8.2 事务边界

- 每个 Command → Domain Event(s) → Projection update 在一个 SQLite 事务中
- Git 操作不在数据库中事务化（Git 自身保证原子性）
- Agent Stream Event 的持久化在单独事务中（允许少量丢失）

### 8.3 不变量检查

恢复完成后执行不变量检查：

```
✅ 所有非终端 Task 的依赖任务都是 MERGED
✅ 所有 LEASED Task 有唯一有效的 WorkspaceLease
✅ 所有 MERGED Task 有非空 commitHash
✅ Project 状态与其所有 Task 状态一致
✅ 没有两个 Task 持有相同路径的活跃 WorkspaceLease
```

不变量失败 → 记录 CRITICAL 日志 → 尝试自动修复 → 无法修复则标记 Project 为 FAILED
