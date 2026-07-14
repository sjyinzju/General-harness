# Event Model — Agent Harness

> **文档类型**: 架构规范
> **版本**: v1.0
> **日期**: 2026-07-14

---

## 1. 概述

Agent Harness 使用事件驱动架构。系统中的所有状态变更通过不可变事件记录。当前状态是从事件流重建的投影（projection）。

---

## 2. 事件类型体系

### 2.1 五种事件类型

| 事件类型 | 来源 | 用途 | 存储 | 不可变 |
|---------|------|------|------|:---:|
| **Command** | 用户/Orchestrator/System | 表达意图（可能失败） | event_log (type=command) | ✅ |
| **Domain Event** | Command Handler | 记录已发生的事实（总是成功） | event_log (type=domain_event) | ✅ |
| **Projection** | Event Store Consumer | 从事件重建的当前状态 | projections 表 | ❌ (可重建) |
| **Audit Event** | Harness 基础设施层 | 安全审计追踪 | audit_log | ✅ |
| **Agent Stream Event** | Agent 子进程 | Agent 运行时的实时流式事件 | agent_events + 文件系统 | ✅ |

---

## 3. Event Envelope

所有持久化事件共用的信封结构：

```typescript
interface EventEnvelope {
  // 标识
  id: string;              // UUID v7（时间排序）
  streamId: string;        // 聚合 ID (projectId / taskId)
  streamVersion: number;   // 乐观锁版本号（stream 内递增）

  // 事件
  eventType: string;       // "ProjectCreated" | "TaskDispatched" | ...
  eventVersion: number;    // Schema 版本（用于 migration）
  payload: unknown;        // 具体事件数据（经 Zod 校验）

  // 元数据
  commandId?: string;      // 触发此事件的 command ID
  idempotencyKey: string;  // 幂等键
  correlationId: string;   // 关联 ID（同一用户请求的所有事件共享）
  causationId?: string;    // 直接原因事件 ID

  // 时间
  timestamp: string;       // ISO 8601
  recordedAt: string;      // 写入时间（可能晚于 timestamp）

  // 来源
  source: "harness" | "orchestrator" | "agent" | "user" | "system";
}
```

---

## 4. Command（命令）

### 4.1 定义

Command 表示用户或系统的**意图**。它可能会被拒绝（非法状态转换、权限不足等）。

```typescript
interface Command {
  type: string;              // "CreateProject" | "DispatchTask" | ...
  payload: unknown;          // 命令参数
  idempotencyKey: string;
  correlationId: string;
  actor: "harness" | "orchestrator" | "user" | "system";
  timestamp: string;
}
```

### 4.2 Command 处理流程

```
Command
  → Command Handler
    → 验证 (状态、权限、前置条件)
      → 如果非法: 返回 CommandRejected (记录在 audit_log)
      → 如果合法: 生成 DomainEvent(s)
        → 写入 event_log (事务)
        → 更新 projection
        → 返回 CommandAccepted
```

### 4.3 Core Commands（Foundation Release）

```typescript
type CoreCommand =
  | { type: "CreateProject"; payload: CreateProjectPayload }
  | { type: "SetGoalContract"; payload: SetGoalContractPayload }
  | { type: "ApprovePlan"; payload: ApprovePlanPayload }
  | { type: "CreateTask"; payload: CreateTaskPayload }
  | { type: "DispatchTask"; payload: DispatchTaskPayload }
  | { type: "SubmitTaskResult"; payload: SubmitTaskResultPayload }
  | { type: "VerifyTask"; payload: VerifyTaskPayload }
  | { type: "CommitTask"; payload: CommitTaskPayload }
  | { type: "MergeTask"; payload: MergeTaskPayload }
  | { type: "CancelProject"; payload: CancelProjectPayload }
  | { type: "PauseProject"; payload: PauseProjectPayload }
  | { type: "ResumeProject"; payload: ResumeProjectPayload }
  | { type: "ChangeGoalContract"; payload: ChangeGoalContractPayload };
```

---

## 5. Domain Event（领域事件）

### 5.1 定义

Domain Event 表示**已经发生且不可撤销的事实**。它总是由成功的 Command 处理产生。

### 5.2 Core Domain Events

```typescript
// 项目生命周期
type ProjectCreated = { projectId: string; goalContractVersion: number; };
type GoalContractSet = { projectId: string; version: number; objective: string; /*...*/ };
type PlanApproved = { projectId: string; planVersion: number; };
type ProjectCancelled = { projectId: string; reason: string; };
type ProjectDone = { projectId: string; artifactReferences: ArtifactReference[]; };

// 任务生命周期
type TaskCreated = { taskId: string; projectId: string; goal: string; dependencies: string[]; };
type TaskReady = { taskId: string; };
type TaskLeased = { taskId: string; profileId: string; workspaceLeaseId: string; };
type TaskRunning = { taskId: string; agentSessionId: string; pid: number; };
type TaskSubmitted = { taskId: string; taskResult: TaskResult; };
type TaskVerified = { taskId: string; evidence: VerificationEvidence[]; };
type TaskCommitted = { taskId: string; commitHash: string; };
type TaskMerged = { taskId: string; targetBranch: string; };

// 异常
type TaskFailedRetryable = { taskId: string; reason: string; retryCount: number; };
type TaskFailedTerminal = { taskId: string; reason: string; };
type TaskOrphaned = { taskId: string; lastKnownPid: number; detectedAt: string; };

// Goal Contract 变更
type GoalContractChanged = { projectId: string; changeRequestId: string;
  oldVersion: number; newVersion: number; };
```

---

## 6. Projection（状态投影）

### 6.1 定义

Projection 是从事件流重建的**当前状态视图**。它存储在 `projections` 表中，**随时可以从 event_log 完全重建**。

### 6.2 投影表

```sql
-- project_current_state
CREATE TABLE project_current_state (
  project_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,          -- ProjectStatus
  goal_contract_version INTEGER,
  plan_version INTEGER,
  task_count INTEGER,
  completed_count INTEGER,
  started_at TEXT,
  updated_at TEXT
);

-- task_current_state
CREATE TABLE task_current_state (
  task_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  status TEXT NOT NULL,          -- TaskStatus
  assigned_profile_id TEXT,
  workspace_lease_id TEXT,
  commit_hash TEXT,
  retry_count INTEGER DEFAULT 0,
  last_error TEXT,
  started_at TEXT,
  completed_at TEXT
);
```

### 6.3 投影更新规则

- Projection 在**同一事务**中与 Domain Event 一起更新
- 如果投影损坏，可以从 event_log **完全重建**
- 不会跨 projection 做 JOIN（保持简单），需要关联数据时由 application 层组装

---

## 7. Audit Event（审计事件）

### 7.1 定义

Audit Event 记录安全相关操作，用于事后审计和合规。与 Domain Event 不同，Audit Event 可能记录"被拒绝的操作"。

### 7.2 Core Audit Events

```typescript
type AuditEvent =
  | { type: "CommandExecuted"; command: Command; exitCode: number; durationMs: number;
      stdoutRef: string; stderrRef: string; }
  | { type: "FileAccessed"; path: string; mode: "read" | "write"; taskId: string; }
  | { type: "SecretScanResult"; taskId: string; found: boolean; findings?: string[]; }
  | { type: "PathViolation"; taskId: string; path: string; allowedPaths: string[]; }
  | { type: "CommandRejected"; command: Command; reason: string; }
  | { type: "BudgetExceeded"; taskId: string; limit: number; actual: number; };
```

---

## 8. Agent Stream Event（Agent 流式事件）

### 8.1 定义

Agent Stream Event 是 Agent 子进程在执行过程中实时发出的流式事件。

### 8.2 统一 AgentEvent 格式

```typescript
type AgentEvent =
  | { type: "session_start"; sessionId: string; profileId: string; timestamp: string; }
  | { type: "assistant_message"; content: string; timestamp: string; }
  | { type: "thinking"; content: string; timestamp: string; }
  | { type: "tool_use"; toolName: string; toolInput: unknown; toolUseId: string; timestamp: string; }
  | { type: "tool_result"; toolUseId: string; isError: boolean; content: string; timestamp: string; }
  | { type: "error"; message: string; code?: string; timestamp: string; }
  | { type: "result"; content: string; isError: boolean; timestamp: string; }
  | { type: "session_end"; sessionId: string; timestamp: string; };
```

### 8.3 与 Domain Event 的关系

- Agent Stream Event 是**原始数据**，由 Adapter 解析后发出
- Domain Event 是**业务事实**，由 Command Handler 在 Agent session 结束后产生
- Agent Stream Event 存储在 `agent_events` 表和文件系统日志中
- Domain Event 存储在 `event_log` 表中并驱动 projection

---

## 9. 持久化 Schema

### 9.1 event_log 表

```sql
CREATE TABLE event_log (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL,
  stream_version INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  event_version INTEGER NOT NULL DEFAULT 1,
  payload TEXT NOT NULL,              -- JSON
  command_id TEXT,
  idempotency_key TEXT NOT NULL UNIQUE,
  correlation_id TEXT NOT NULL,
  causation_id TEXT,
  timestamp TEXT NOT NULL,            -- ISO 8601
  recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
  source TEXT NOT NULL
);

CREATE INDEX idx_event_log_stream ON event_log(stream_id, stream_version);
CREATE INDEX idx_event_log_correlation ON event_log(correlation_id);
```

### 9.2 audit_log 表

```sql
CREATE TABLE audit_log (
  id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,              -- JSON
  timestamp TEXT NOT NULL,
  task_id TEXT,
  project_id TEXT
);

CREATE INDEX idx_audit_log_task ON audit_log(task_id);
```

### 9.3 agent_events 表

```sql
CREATE TABLE agent_events (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,              -- JSON
  timestamp TEXT NOT NULL,
  recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_agent_events_session ON agent_events(session_id);
```

---

## 10. 事件版本化与 Migration

- 每个 `eventType` 有独立的 schema version
- 新增字段：递增 minor version，新字段有默认值
- 删除/重命名字段：递增 major version，提供 up/down migration 函数
- Projection 重建时，使用最新的 event schema 和 migration 链
- Schema 变更记录在 `schema_migrations` 表中

---

## 11. 最小事件类型集（Foundation Release）

Foundation Release 不需要实现完整的事件目录。以下是最小集：

```
Commands:
  CreateProject, SetGoalContract, ApprovePlan,
  CreateTask, DispatchTask, SubmitTaskResult,
  VerifyTask, CommitTask, MergeTask, CancelProject

Domain Events:
  ProjectCreated, GoalContractSet, PlanApproved,
  TaskCreated, TaskReady, TaskLeased, TaskRunning,
  TaskSubmitted, TaskVerified, TaskCommitted, TaskMerged,
  TaskFailedRetryable, TaskFailedTerminal, TaskOrphaned,
  ProjectCancelled, ProjectDone

Audit Events:
  CommandExecuted, PathViolation, SecretScanResult, BudgetExceeded

Agent Events:
  session_start, assistant_message, tool_use, tool_result,
  error, result, session_end
```
