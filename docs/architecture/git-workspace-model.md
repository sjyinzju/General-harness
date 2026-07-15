# Git Workspace Model v3 — Agent Harness

> **版本**: v3.0
> **日期**: 2026-07-15
> **修订**: Operation/Saga, 禁止空 commit, 冲突修复流程, orphan 验证

---

## 1. Operation ID 集成

所有 Git 写操作通过 Operation/Saga 模型:

```
Phase 1: INSERT operation (PENDING) → 获取 operation_id
Phase 2: git {commit|merge|cherry-pick|worktree}
Phase 3: UPDATE operation (COMPLETED|FAILED)
```

Git commit message trailer:

```
harness: TASK-014 - Implement OAuth callback

harness-operation-id: op-a1b2c3d4e5
harness-task-id: TASK-014
harness-execution-id: exec-f6g7h8i9
harness-project-id: proj-001
```

---

## 2. Commit 规则

### 2.1 禁止空 Commit

```
git diff --stat → 无变更:
  ❌ git commit --allow-empty (禁止)
  
  根据 Task 类型:
    只读分析/审查 Task → CommitOperation.result = NO_CHANGES → 正常
    写入 Task → VerificationJob → FAILED → retry
```

### 2.2 Commit 流程

```
1. Agent 退出, Execution → COMPLETED
2. git diff (worktree 内)
3. 文件范围检查 (diff 内容在 allowedPaths 内)
4. 密钥扫描 (diff 内容无密钥)
5. 检查无 .git 目录修改
6. 如有变更 → git add (allowedPaths 内文件) → git commit (含 operation_id trailer)
7. 如无变更 → 根据 Task 类型决定
8. CommitOperation → COMPLETED
9. 写入 commit_hash 到 commit_operations 表
```

---

## 3. 合并与冲突

### 3.1 禁止自动切换策略

```
git cherry-pick 冲突:
  ❌ 不尝试 git merge (禁止自动切换策略)
  
  1. git cherry-pick --abort
  2. IntegrationJob → CONFLICT
  3. 创建 IntegrationRepairTask
  4. 分配新 Execution Attempt (可以是不同 Agent)
  5. Agent 修复冲突
  6. 修复 commit 通过 VERIFIED
  7. 新 IntegrationJob 合并修复 commit
```

### 3.2 IntegrationJob 流程

```
N 个 Task 全部 VERIFIED → 触发 IntegrationJob:
  1. 按依赖顺序排列 commits
  2. 对每个 commit: git cherry-pick
  3. 全部成功 → 运行集成测试
  4. 测试通过 → IntegrationJob → COMPLETED
  5. 所有 batch task → DONE
```

---

## 4. Orphan Worktree 清理

### 4.1 Ownership Marker

```
.harness/worktrees/TASK-014-auth-callback/
  └── .harness-owner
      内容: {
        "run_id": "proj-001",
        "task_id": "TASK-014",
        "operation_id": "op-xxx",
        "created_at": "2026-07-15T10:00:00Z"
      }
```

### 4.2 清理验证

```
Reconciliation 检测到 orphan worktree:
  1. 读取 .harness-owner (ownership marker)
  2. 验证 branch name 以 harness/ 开头
  3. 检查 operation_id:
     a. 查询 operations 表 → COMPLETED → 可以安全清理
     b. PENDING/RUNNING → reconciliation 处理 operation
     c. 不存在 → 标记为 SUSPICIOUS, 不自动清理
  4. 通过 → git worktree remove + git branch -D
  5. 失败 → 记录 audit log + 人工介入
```

---

## 5. WorkspaceLease 生命周期

```
Task DISPATCHED → WorkspaceLease::ACQUIRED
  → Agent 进程开始 → WorkspaceLease::ACTIVE
  → Task 完成或取消 → WorkspaceLease::RELEASED
  → Supervisor 崩溃 → WorkspaceLease::EXPIRED (由 reconciliation 检测)

EXPIRED lease:
  - worktree 可能仍然存在 (orphan)
  - reconciliation 执行 orphan cleanup
```

---

## 6. Worktree 命名 (不变)

```
.harness/worktrees/TASK-014-auth-callback/
.harness/worktrees/TASK-018-dashboard-shell/
```

仅绑定 Task ID + 描述。Agent/Model 信息在数据库中。
