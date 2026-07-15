# Integration Queue Model — Agent Harness

> **版本**: v1.0
> **日期**: 2026-07-15
> **状态**: 待审批

---

## 1. 概述

Integration Queue 管理已 VERIFIED 的 Task 的合并工作。每个 IntegrationJob 是独立的 Operation，有自己的 Execution Attempt 和生命周期。

---

## 2. Task 状态模型变更

Task 主生命周期**不再包含** COMMITTED、MERGING、MERGED。

### 2.1 Task 简化后状态

```
PENDING → READY → DISPATCHED → RUNNING → AWAITING_INPUT → SUBMITTED → VERIFIED → DONE
Terminal: CANCELLED | SUPERSEDED | FAILED
```

### 2.2 分离出的独立实体

| 旧 Task 状态 | 新模型 |
|-------------|--------|
| LEASED | → `WorkspaceLease` 状态: ACQUIRED |
| COMMITTED | → `CommitOperation` 状态: COMPLETED |
| MERGING | → `IntegrationJob` 状态: RUNNING |
| MERGED | → `IntegrationJob` 状态: COMPLETED |

---

## 3. IntegrationJob

### 3.1 生命周期

```
CREATED → QUEUED → RUNNING → COMPLETED
                           → FAILED
                           → CONFLICT
```

### 3.2 结构

```rust
struct IntegrationJob {
    id: String,
    project_id: String,
    /// 待合并的 task IDs (按依赖顺序)
    task_ids: Vec<String>,
    /// 逐个 task 的 commit_hash
    commits: Vec<String>,
    target_branch: String,   // integration/{project_id}
    status: IntegrationJobStatus,
    /// 如果 CONFLICT, 记录冲突文件
    conflict_files: Vec<String>,
    /// 当前或最近的 Execution Attempt
    execution_attempt_id: Option<String>,
    created_at: DateTime<Utc>,
}
```

### 3.3 合并流程

```
1. 一组 task 全部 VERIFIED → 创建 IntegrationJob
2. IntegrationJob → QUEUED
3. Scheduler 分配 IntegrationJob → RUNNING
4. 对每个 task.commit_hash:
   a. git cherry-pick {hash}
   b. 如果冲突:
      → IntegrationJob → CONFLICT
      → 创建 IntegrationRepairTask
      → 分配新的 Execution Attempt 修复冲突
      → 修复完成后重新 IntegrationJob
   c. 如果成功: 继续下一个
5. 所有 cherry-pick 成功 → 运行集成测试
6. 集成测试通过 → IntegrationJob → COMPLETED
7. 每个源 task → DONE
```

### 3.4 冲突处理

**不得自动切换合并策略。**

冲突时的流程:
1. git cherry-pick --abort
2. IntegrationJob → CONFLICT
3. 创建 `IntegrationRepairTask`:
   - goal: "解决 src/auth/callback.ts 和 packages/shared/types.ts 的合并冲突"
   - resource_claims: 冲突文件 (write)
   - depends_on: 冲突的源 task
4. 分配新的 Execution Attempt
5. 修复 → VERIFIED → 重新创建 IntegrationJob (仅包含修复 commit)
6. 新 IntegrationJob 开始

---

## 4. CommitOperation

```rust
struct CommitOperation {
    id: String,
    task_id: String,
    execution_attempt_id: String,
    operation_id: String,     // Operation/Saga 的 operation_id
    status: CommitOperationStatus,
    commit_hash: Option<String>,
    has_changes: bool,        // false 如果 Agent 无文件变更
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
}
```

### 无 diff 任务的处理

```
Agent 退出，git diff 无变更:
  → CommitOperation 不创建 commit
  → 不创建空 commit (禁止 git commit --allow-empty)
  → 根据任务类型决定:
    - 只读分析任务 (explore/review): NO_CHANGES → 正常完成
    - 代码生成任务 (implement): VERIFICATION_FAILED → 重试
  → 原因记录在 CommitOperation.result_reason
```

---

## 5. 数据库表

```sql
CREATE TABLE integration_jobs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  task_ids_json TEXT NOT NULL,
  commits_json TEXT NOT NULL,
  target_branch TEXT NOT NULL,
  status TEXT NOT NULL,
  conflict_files_json TEXT,
  execution_attempt_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE commit_operations (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  execution_attempt_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  status TEXT NOT NULL,
  commit_hash TEXT,
  has_changes INTEGER NOT NULL DEFAULT 0,
  result_reason TEXT,
  started_at TEXT NOT NULL,
  completed_at TEXT
);
```
