# Recovery Model v3 — Agent Harness

> **版本**: v3.0
> **日期**: 2026-07-15
> **修订**: LOST vs ORPHANED, Supervisor 崩溃恢复, Operation reconciliation

---

## 1. 两种异常类型

| 类型 | 原因 | 谁检测 | Agent 子进程 | 恢复方式 |
|------|------|--------|-------------|---------|
| **ORPHANED** | Agent 子进程自身异常退出 | Supervisor (仍在运行) | 已退出 | 同 Execution chain: retry 或 fail |
| **LOST** | Supervisor 崩溃 | 新 Supervisor (启动时) | 被 watchdog 终止 | 新建 Execution + `--resume` (保存的 native_session_id) |

---

## 2. Supervisor 崩溃恢复

### 2.1 假设

- **不能重新接管仍存活的 Agent 子进程** — stdin/stdout pipe 已断开
- Agent 子进程通过 watchdog 终止
- 如果 supervisor 崩溃, Agent 子进程**必定**被终止

### 2.2 恢复流程

```
1. 用户执行 harness attach {run_id}
2. 检测 supervisor.json:
   a. PID 不存在 → supervisor 已崩溃
3. 新 supervisor 启动
4. 读取 SQLite current_state
5. Reconciliation:
   a. 查询 execution_attempts WHERE lifecycle IN ('CREATED', 'RUNNING')
   b. 对每个:
      → lifecycle = LOST
      → 原因 = 'supervisor_crash'
   c. 对每个关联的 WorkspaceLease:
      → lifecycle = EXPIRED
6. 对每个 LOST Execution:
   a. retry_count < max:
      → 创建新 Execution Attempt (attempt_number + 1)
      → 设置 native_session_id = 旧 Execution 的 session_id
      → 新 Execution → CREATED → RUNNING (--resume)
   b. retry_count ≥ max:
      → Execution → FAILED
      → Task → FAILED
7. 清理 orphan worktree (参见 git-workspace-model.md §4)
8. 新 supervisor 写入 supervisor.json
9. 继续正常运作
```

### 2.3 Watchdog 机制

```
Supervisor 启动 → spawn watchdog 子进程

Watchdog:
  - 唯一职责: 检测父进程 (supervisor) 是否存活
  - 不操作 SQLite, Git, Adapter
  - 父进程死亡 → 枚举 Agent 子进程 (从 execution_attempts 表 + 进程树)
    → SIGTERM → 5s timeout → SIGKILL
  - Windows: Job Object 自动终止 (不依赖 watchdog)
  - 退出

Agent 子进程全部终止 → Execution → LOST
```

---

## 3. Operation Reconciliation

### 3.1 长期 PENDING/RUNNING Operations

```
启动时:
  SELECT * FROM operations
  WHERE status IN ('PENDING', 'RUNNING')
    AND started_at < datetime('now', '-60 seconds')

对每个:
  git_commit operation:
    → git log --grep "harness-operation-id: {id}"
    → 找到 commit → UPDATE COMPLETED + commit_hash
    → 未找到 → 检查 worktree 状态 → 重试或 FAILED

  git_merge / git_cherry_pick operation:
    → git branch --contains {commit_hash}
    → 包含 → UPDATE COMPLETED
    → 不包含 → 重试或 FAILED

  acceptance_check operation:
    → 检查测试输出文件是否存在
    → 存在 + exit code 已知 → UPDATE COMPLETED/FAILED
    → 不存在 → 重试
```

### 3.2 幂等重试

```
重试执行前:
  1. 检查 operation 是否已 COMPLETED (可能被之前的 reconciliation 完成)
  2. 检查外部是否有完成证据 (git trailer, test output)
  3. 有证据 → 补充数据库记录
  4. 无证据 → 重新执行 (幂等操作)
```

---

## 4. 数据库成功但外部失败 — 处理

```
Scenario: Phase 3 COMMIT 成功 (operations SET COMPLETED), 但 git 实际未完成

原因: 少见 — Phase 3 只做数据库写入。如果是 Phase 2 极早退出:
  - git 操作返回成功但实际未落盘 → 这是 git 自身的保证
  - 如果 git 返回失败 → Phase 3 记录 FAILED

如果在 Phase 2 和 Phase 3 之间崩溃:
  - Operation 状态 = RUNNING (Phase 1 写入)
  - Reconciliation → 检测到操作未完成 → 重试 (幂等)
```

---

## 5. 并发 Execution 恢复

```
Task A: Execution 1 LOST → 新 Execution 2 (--resume)
Task B: Execution 1 COMPLETED (正常完成)

恢复:
  Task A: 新 Execution 开始 → 从 SQLite 读取 native_session_id → --resume
  Task B: 已 COMPLETED → 继续 SUBMITTED → VERIFIED 流程

Task A 和 B 的恢复独立，不互相影响
```
