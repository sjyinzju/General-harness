# Resource Claim Model — Agent Harness

> **版本**: v1.0
> **日期**: 2026-07-15
> **状态**: 待审批

---

## 1. 概述

Resource Claim 是并发控制机制。Scheduler 在分发 Task 时必须**原子性地**检查并获取三类资源：

1. **Profile Slot** — Agent 并发槽位
2. **Workspace Lease** — Worktree 互斥访问
3. **Resource Claim** — 文件/目录/逻辑资源的读/写冲突

三者必须在同一个调度决策中通过，不能分步。

---

## 2. Resource 类型

```rust
enum Resource {
    /// 精确文件路径
    File(PathBuf),
    /// 目录前缀 (匹配该目录下所有文件)
    Directory(PathBuf),
    /// 整个 Git 仓库
    Repo,
    /// 逻辑资源
    Logical(LogicalResource),
}

enum LogicalResource {
    /// 依赖清单文件 (package.json, Cargo.toml, etc.)
    DependencyManifest,
    /// 数据库 schema
    DatabaseSchema,
    /// 集成分支
    IntegrationBranch,
    /// 共享类型/接口文件
    SharedTypes,
    /// 配置文件
    Configuration,
}
```

## 3. Access Mode

```rust
enum AccessMode {
    /// 只读 — 与 READ 兼容，与 WRITE 冲突
    Read,
    /// 写入 — 与 READ 和 WRITE 都冲突
    Write,
}
```

**冲突矩阵**:

|  | READ | WRITE |
|---|:---:|:---:|
| **READ** | ✅ 兼容 | ❌ 冲突 |
| **WRITE** | ❌ 冲突 | ❌ 冲突 |

## 4. Claim 生命周期

```
Task 进入 READY
  │
  ▼
Scheduler 评估:
  1. Profile slot 可用?
  2. WorkspaceLease 可获取?
  3. 所有 required Resource Claim 可获取 (无冲突)?
  │
  ├─ 全部通过 → 原子获取全部三个 → Task DISPATCHED
  └─ 任一失败 → Task 保持 READY，等待下次调度
  │
  ▼
Task 完成 (VERIFIED, terminal):
  → 释放 Profile slot
  → 释放 WorkspaceLease
  → 释放 Resource Claims
```

## 5. Claim 声明

Task 定义时声明其资源需求：

```json
{
  "task_id": "TASK-014",
  "resource_claims": [
    { "resource": { "type": "directory", "path": "src/auth/" }, "mode": "write" },
    { "resource": { "type": "directory", "path": "packages/shared/" }, "mode": "read" },
    { "resource": { "type": "logical", "name": "database_schema" }, "mode": "read" },
    { "resource": { "type": "logical", "name": "dependency_manifest" }, "mode": "write" }
  ]
}
```

## 6. 原子调度

```rust
impl Scheduler {
    async fn try_dispatch(&self, task: &Task) -> Result<DispatchResult> {
        // 在单个决策上下文中原子检查:
        let mut tx = self.db.begin_transaction()?;

        // 1. Profile slot
        let slot = self.try_acquire_profile_slot(&task.assigned_profile_id, &mut tx)?;
        if slot.is_none() { return Ok(DispatchResult::NoSlot); }

        // 2. Workspace lease
        let lease = self.try_acquire_workspace_lease(&task.id, &mut tx)?;
        if lease.is_none() { return Ok(DispatchResult::NoLease); }

        // 3. Resource claims
        for claim in &task.resource_claims {
            if self.has_conflict(claim, &mut tx)? {
                return Ok(DispatchResult::ResourceConflict {
                    resource: claim.resource.clone(),
                    conflict_with: self.get_conflict_owner(claim, &mut tx)?,
                });
            }
        }

        // 全部通过 → 原子提交
        self.insert_claims(&task.id, &task.resource_claims, &mut tx)?;
        self.update_task_status(&task.id, TaskStatus::Dispatched, &mut tx)?;
        tx.commit()?;

        Ok(DispatchResult::Dispatched)
    }
}
```

## 7. Foundation vs Functional

| 能力 | Foundation | Functional |
|------|:---:|:---:|
| Profile slot | ✅ | ✅ |
| Workspace lease | ✅ | ✅ |
| File claims | ✅ | ✅ |
| Directory claims | ✅ | ✅ |
| Repo claims | ✅ | ✅ |
| Logical resource claims | ✅ | ✅ |
| 自动从 scope 推导 claims | ❌ | ✅ |
| 动态冲突检测 (scope expansion) | ❌ | ✅ |
| Read/Write 升级 | ❌ | ✅ |

Foundation: Task 必须**显式声明** resource_claims。不做自动推导。

---

## 8. 存储

```sql
CREATE TABLE resource_claims (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  resource_type TEXT NOT NULL,    -- 'file' | 'directory' | 'repo' | 'logical'
  resource_path TEXT,             -- 文件/目录路径 (repo/logical 为 NULL)
  resource_name TEXT,             -- logical 资源名称
  access_mode TEXT NOT NULL,      -- 'read' | 'write'
  status TEXT NOT NULL DEFAULT 'active',  -- 'active' | 'released'
  acquired_at TEXT NOT NULL,
  released_at TEXT
);

CREATE INDEX idx_resource_claims_active
  ON resource_claims(resource_type, resource_path, resource_name)
  WHERE status = 'active';
```
