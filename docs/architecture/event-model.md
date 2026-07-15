# Event & Operation Model v3 — Agent Harness

> **版本**: v3.0
> **日期**: 2026-07-15
> **修订**: Operation/Saga 模型 + 四套生命周期事件 + idempotency

---

## 1. Operation / Saga 流程

### 1.1 三阶段

```
Phase 1: RECORD INTENT
  BEGIN TRANSACTION;
    INSERT INTO operations (id, operation_id, type, status='PENDING', payload);
    INSERT INTO event_log (operation_started);
  COMMIT;
  → 返回 operation_id 给调用方

Phase 2: EXECUTE
  执行实际副作用 (git, filesystem, process)
  Git 操作写入 trailer: harness-operation-id: {operation_id}

Phase 3: RECORD RESULT
  BEGIN TRANSACTION;
    UPDATE operations SET status='COMPLETED'|'FAILED', result=...;
    INSERT INTO event_log (operation_completed|operation_failed);
  COMMIT;
```

### 1.2 Reconciliation

```
启动时或定时:
  查找 operations WHERE status IN ('PENDING', 'RUNNING')
    AND started_at < NOW() - N seconds

  对每个:
    1. 检查外部副作用是否实际完成:
       git_commit → git log --grep "harness-operation-id: {id}"
       git_merge → git branch --contains {commit_hash}
       acceptance_check → 检查测试输出文件
    2. 完成 → UPDATE operations SET status='COMPLETED' + INSERT event_log
    3. 未完成 → 重新执行 (幂等) 或标记 FAILED
```

---

## 2. 数据库表

### 2.1 Operations

```sql
CREATE TABLE operations (
  id TEXT PRIMARY KEY,
  operation_id TEXT NOT NULL UNIQUE,
  operation_type TEXT NOT NULL,
  -- 'git_commit' | 'git_merge' | 'git_cherry_pick' |
  -- 'git_worktree_create' | 'git_worktree_remove' |
  -- 'acceptance_check' | 'integration_test'
  task_id TEXT NOT NULL,
  execution_attempt_id TEXT,
  status TEXT NOT NULL,  -- PENDING | RUNNING | COMPLETED | FAILED
  payload_json TEXT NOT NULL,
  result_json TEXT,
  idempotency_key TEXT NOT NULL UNIQUE,
  started_at TEXT NOT NULL,
  completed_at TEXT
);
```

### 2.2 四套生命周期表

```sql
-- Projects (current state)
CREATE TABLE projects (
  id TEXT PRIMARY KEY,
  objective TEXT NOT NULL,
  lifecycle TEXT NOT NULL,
  health TEXT NOT NULL DEFAULT 'HEALTHY',
  waiting_on TEXT NOT NULL DEFAULT 'NONE',
  pause TEXT NOT NULL DEFAULT 'NONE',
  reason TEXT,
  goal_contract_version INTEGER,
  plan_version INTEGER,
  version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

-- Tasks (current state)
CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  goal TEXT NOT NULL,
  lifecycle TEXT NOT NULL,
  health TEXT NOT NULL DEFAULT 'HEALTHY',
  waiting_on TEXT NOT NULL DEFAULT 'NONE',
  retry_count INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 3,
  reason TEXT,
  assigned_profile_id TEXT,
  dependencies_json TEXT NOT NULL DEFAULT '[]',
  resource_claims_json TEXT NOT NULL DEFAULT '[]',
  acceptance_checks_json TEXT NOT NULL DEFAULT '[]',
  version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

-- Execution Attempts
CREATE TABLE execution_attempts (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  attempt_number INTEGER NOT NULL,
  lifecycle TEXT NOT NULL,  -- CREATED | RUNNING | COMPLETED | FAILED | LOST | CANCELLED
  profile_id TEXT NOT NULL,
  agent_session_id TEXT,
  native_session_id TEXT,   -- Claude --resume / Codex thread_id
  pid INTEGER,              -- Agent 子进程 PID
  started_at TEXT,
  completed_at TEXT,
  reason TEXT               -- LOST: 'supervisor_crash' | FAILED: 'agent_error' | ...
);

-- Workspace Leases
CREATE TABLE workspace_leases (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  lifecycle TEXT NOT NULL,  -- ACQUIRED | ACTIVE | RELEASED | EXPIRED
  worktree_path TEXT NOT NULL,
  branch_name TEXT NOT NULL,
  acquired_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  released_at TEXT
);
```

### 2.3 局部实体表

```sql
CREATE TABLE verification_jobs (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  status TEXT NOT NULL,  -- PENDING | RUNNING | PASSED | FAILED
  checks_json TEXT NOT NULL,
  evidence_paths_json TEXT,
  started_at TEXT,
  completed_at TEXT
);

CREATE TABLE commit_operations (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  operation_id TEXT NOT NULL REFERENCES operations(operation_id),
  status TEXT NOT NULL,  -- PENDING | RUNNING | COMPLETED | FAILED | NO_CHANGES
  commit_hash TEXT,
  has_changes INTEGER NOT NULL DEFAULT 0,
  result_reason TEXT,
  started_at TEXT NOT NULL,
  completed_at TEXT
);

CREATE TABLE integration_jobs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  task_ids_json TEXT NOT NULL,
  commits_json TEXT NOT NULL,
  target_branch TEXT NOT NULL,
  status TEXT NOT NULL,  -- CREATED | QUEUED | RUNNING | COMPLETED | CONFLICT | FAILED
  conflict_files_json TEXT,
  execution_attempt_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE resource_claims (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  resource_type TEXT NOT NULL,
  resource_path TEXT,
  resource_name TEXT,
  access_mode TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  acquired_at TEXT NOT NULL,
  released_at TEXT
);
```

---

## 3. Event Log (不变)

```sql
CREATE TABLE event_log (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  correlation_id TEXT NOT NULL,
  timestamp TEXT NOT NULL,
  recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
  source TEXT NOT NULL
);
```

### 新增事件类型

```rust
// Operation 事件
OperationStarted { operation_id, operation_type, task_id },
OperationCompleted { operation_id, result },
OperationFailed { operation_id, reason },

// Execution 事件
ExecutionCreated { execution_id, task_id, attempt_number },
ExecutionRunning { execution_id, pid },
ExecutionCompleted { execution_id },
ExecutionFailed { execution_id, reason },
ExecutionLost { execution_id, reason },  // ← supervisor crash
ExecutionCancelled { execution_id },

// Workspace 事件
WorkspaceLeaseAcquired { lease_id, task_id, worktree_path },
WorkspaceLeaseReleased { lease_id },
WorkspaceLeaseExpired { lease_id },
```

---

## 4. Idempotency

```
所有 Command → Operation/Saga → operation_id

Phase 1: operation_id 由调用方生成 (UUID v7)
  → 如果 operation_id 已存在于 operations 表 → 返回已有结果 (幂等)
  → 否则 INSERT + 执行

Phase 3: 结果写入
  → 如果 operation 已经是 COMPLETED/FAILED → 幂等返回
  → 否则 UPDATE

Git trailer 包含 operation_id → 事后可追溯
```

## 5. 事务边界

```
Phase 1 transaction: operations INSERT + event_log INSERT
  (纯数据库操作, 无外部副作用)

Phase 2: 纯外部操作 (git, filesystem)
  (无数据库操作)

Phase 3 transaction: operations UPDATE + event_log INSERT
  (纯数据库操作, 记录外部操作结果)
```

数据库提交成功但外部副作用失败 → reconciliation 处理。
外部副作用成功但数据库提交失败 → reconciliation 检测到 Git 中有 operation_id trailer → 补写数据库。
