# State Machines v3 — Agent Harness

> **版本**: v3.0
> **日期**: 2026-07-15
> **修订**: 四套核心生命周期 + 局部状态化实体

---

## 1. 四套核心生命周期

### 1.1 状态数量总览

| 生命周期 | 非终端 | 终端 | 总计 |
|---------|:-----:|:---:|:---:|
| Project | 10 | 2 (CANCELLED, FAILED) | 12 |
| Task | 7 | 4 (DONE, CANCELLED, SUPERSEDED, FAILED) | 11 |
| Execution Attempt | 2 | 4 (COMPLETED, FAILED, LOST, CANCELLED) | 6 |
| Workspace Lease | 2 | 2 (RELEASED, EXPIRED) | 4 |

**`is_terminal()` 规则**: 状态进入后不可再转换到任何其他状态。终端状态禁止除审计查询外的所有操作。

**允许 retry 的状态**: FAILED (Task), FAILED (Execution), LOST (Execution)
**允许恢复的状态**: PAUSED (Project 辅助维度 pause)

### 1.2 Project

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectLifecycle {
    Created,
    Clarifying,
    GoalLocked,
    Planning,
    AwaitingApproval,
    Active,
    Integrating,
    Verifying,
    Delivering,
    // ── Terminal ──
    Done,
    Cancelled,
    Failed,
}

impl ProjectLifecycle {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done | Self::Cancelled | Self::Failed)
    }

    pub fn valid_successors(&self) -> &[Self] { /* see table below */ }
}
```

| 状态 | 合法后继 |
|------|---------|
| Created | Clarifying |
| Clarifying | GoalLocked |
| GoalLocked | Planning |
| Planning | AwaitingApproval |
| AwaitingApproval | Active, Planning, Cancelled |
| Active | Integrating, Failed, Cancelled |
| Integrating | Verifying, Active, Cancelled |
| Verifying | Delivering, Active, Cancelled |
| Delivering | Done |
| Done | *(terminal)* |
| Cancelled | *(terminal)* |
| Failed | *(terminal)* |

**辅助维度**: `health`, `waiting_on`, `pause`, `reason` (独立于 lifecycle)

### 1.3 Task

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskLifecycle {
    Pending,
    Ready,
    Dispatched,
    Running,
    AwaitingInput,
    Submitted,
    Verified,
    // ── Terminal ──
    Done,
    Cancelled,
    Superseded,
    Failed,
}

impl TaskLifecycle {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done | Self::Cancelled | Self::Superseded | Self::Failed)
    }

    pub fn allows_retry(&self) -> bool {
        matches!(self, Self::Failed)
    }
}
```

| 状态 | 合法后继 |
|------|---------|
| Pending | Ready, Superseded, Cancelled |
| Ready | Dispatched, Cancelled |
| Dispatched | Running, Pending, Failed |
| Running | AwaitingInput, Submitted, Pending, Failed |
| AwaitingInput | Running, Failed |
| Submitted | Verified, Pending |
| Verified | Done |
| Done | *(terminal)* |
| Cancelled | *(terminal)* |
| Superseded | *(terminal)* |
| Failed | *(terminal, but allows retry → new Task)* |

### 1.4 Execution Attempt

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionLifecycle {
    Created,
    Running,
    // ── Terminal ──
    Completed,
    Failed,
    Lost,
    Cancelled,
}

impl ExecutionLifecycle {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Lost | Self::Cancelled)
    }

    pub fn allows_retry(&self) -> bool {
        matches!(self, Self::Failed | Self::Lost)
    }
}
```

| 状态 | 合法后继 |
|------|---------|
| Created | Running, Failed, Cancelled |
| Running | Completed, Failed, Lost, Cancelled |
| Completed | *(terminal)* |
| Failed | *(terminal, allows retry → new Execution)* |
| Lost | *(terminal, allows retry → new Execution with --resume)* |
| Cancelled | *(terminal)* |

### 1.5 Workspace Lease

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseLifecycle {
    Acquired,
    Active,
    // ── Terminal ──
    Released,
    Expired,
}

impl LeaseLifecycle {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Released | Self::Expired)
    }
}
```

| 状态 | 合法后继 |
|------|---------|
| Acquired | Active, Expired |
| Active | Released, Expired |
| Released | *(terminal)* |
| Expired | *(terminal)* |

---

## 2. 局部状态化实体

| 实体 | 状态 | 父实体 |
|------|------|--------|
| `VerificationJob` | PENDING → RUNNING → PASSED / FAILED | Task |
| `CommitOperation` | PENDING → RUNNING → COMPLETED / FAILED / NO_CHANGES | Task |
| `IntegrationJob` | CREATED → QUEUED → RUNNING → COMPLETED / CONFLICT / FAILED | Project |
| `ApprovalRequest` | PENDING → APPROVED / REJECTED | Project |
| `ResourceClaim` | ACTIVE → RELEASED | Task |
| `ChangeRequest` | PROPOSED → APPROVED / REJECTED | Project |
| `IntegrationRepairTask` | PENDING → READY → DISPATCHED → ... → DONE | IntegrationJob |

---

## 3. Execution Attempt 与 Task 的关系

- 一个 Task 可以有**多个** Execution Attempt (retry)
- 同一时间最多**一个** RUNNING
- Execution 通过 `--resume` 延续之前的会话 (LOST 场景)
- Execution 失败后可以由 Scheduler 决定创建新 Execution (retry) 或 Task 进入 FAILED

---

## 4. 关键状态转换

### Task: PENDING → READY
条件: 所有 `depends_on` tasks 的 status = DONE

### Task: READY → DISPATCHED
条件: Scheduler 原子获取 profile slot + workspace lease + resource claims

### Task: DISPATCHED → RUNNING
条件: Execution Attempt 进入 RUNNING

### Task: RUNNING → SUBMITTED
条件: Execution Attempt → COMPLETED, TaskResult 已接收

### Task: SUBMITTED → VERIFIED
条件: VerificationJob → PASSED

### Task: VERIFIED → DONE
条件: CommitOperation → COMPLETED, IntegrationJob → COMPLETED

### Execution: RUNNING → LOST
条件: Supervisor 崩溃, watchdog 终止 Agent 子进程
处理: 保存 native_session_id, 创建新 Execution + --resume

---

## 5. 辅助维度非法组合

| lifecycle | health | 非法? | 原因 |
|-----------|--------|:---:|------|
| CREATED | DEGRADED | ✅ 非法 | 无 Agent 运行 |
| AWAITING_APPROVAL | PAUSED | ✅ 非法 | 未执行 |
| DONE | any waiting_on | ✅ 非法 | 已完成 |
| PENDING | STALLED | ✅ 非法 | 未开始 |
| CANCELLED | PAUSED | ✅ 非法 | 已取消 |

---

## 6. 状态不变量

```
Project:
  ACTIVE → 至少 1 Task DISPATCHED/RUNNING 或 全部 DONE/terminal
  INTEGRATING → 至少 1 IntegrationJob RUNNING/COMPLETED

Task:
  DISPATCHED → WorkspaceLease 存在且 ACTIVE
  RUNNING → 恰好 1 Execution Attempt RUNNING
  DONE → CommitOperation COMPLETED + IntegrationJob COMPLETED
  FAILED → 所有 Execution Attempt terminal (FAILED/LOST/CANCELLED) + retry ≥ max

Execution:
  LOST → 对应的 Agent 子进程已被终止

WorkspaceLease:
  ACTIVE → worktree 目录存在 + branch 存在
  EXPIRED → 无 Task 引用此 lease
```
