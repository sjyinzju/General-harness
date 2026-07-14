# Git Workspace Model — Agent Harness

> **文档类型**: 架构规范
> **版本**: v1.0
> **日期**: 2026-07-14

---

## 1. 核心原则

1. **Worktree 绑定 Task/Workstream，不绑定 Agent 或 Model**
2. **Harness 独占 Git 写操作（add/commit/merge/rebase）**
3. **Worker Agent 默认不得执行 git 命令**
4. **不自动提交用户主工作区的 dirty changes**

---

## 2. Git 分支模型

```
main ───────────────────────────────────────────── (只读参考)
  │
  └── integration/project-001 ────────────── (集成分支)
        │
        ├── harness/TASK-001-init-server ──── (任务分支)
        ├── harness/TASK-002-add-tests ────── (任务分支)
        ├── harness/TASK-003-auth ─────────── (任务分支)
        └── harness/WORKSTREAM-auth ───────── (工作流分支)
```

### 分支命名

```
harness/{taskId}-{short-description}
harness/workstream-{short-description}

示例:
  harness/TASK-014-auth-callback
  harness/TASK-018-dashboard-shell
  harness/workstream-authentication
```

---

## 3. Worktree 生命周期

### 3.1 创建

```typescript
class WorktreeManager {
  /** 为任务创建隔离的 worktree */
  async createForTask(task: Task, baseBranch: string): Promise<Workspace> {
    const branchName = `harness/${task.id}-${slugify(task.goal)}`;
    const worktreePath = `.harness/worktrees/${task.id}-${slugify(task.goal)}/`;

    // 1. 基于 integration 分支创建任务分支
    await git.branch(branchName, baseBranch);

    // 2. 创建 worktree
    await git.worktree.add(worktreePath, branchName);

    // 3. 获取 workspaces lease
    const lease = await this.leaseService.acquire(task.id, worktreePath);

    return { taskId: task.id, worktreePath, branchName, lease };
  }
}
```

### 3.2 使用

- Agent 子进程在 worktree 中执行
- 所有文件修改发生在此 worktree 内
- Worktree 是完整项目副本，Agent 拥有完整语境
- `scope.allowedPaths` 限制可修改的文件范围

### 3.3 清理

```typescript
class WorktreeManager {
  /** 任务成功合并后清理 */
  async cleanup(taskId: string): Promise<void> {
    // 1. 释放 workspace lease
    await this.leaseService.release(taskId);

    // 2. 删除 worktree 目录
    await fs.rm(worktreePath, { recursive: true, force: true });

    // 3. 清理 worktree 的 Git 元数据
    await git.worktree.prune();

    // 4. 可选：删除远程分支引用（本地保留用于审计）
    // 不删除任务分支，保留用于事后检查
  }
}
```

---

## 4. Harness 独占的 Git 操作

### 4.1 Commit 流程

```typescript
class GitCommitService {
  async commitTask(task: Task, workspace: Workspace): Promise<string> {
    // 1. 获取 diff
    const diff = await git.diff(workspace.worktreePath);

    // 2. 验证文件范围（Layer 5）
    const scopeResult = await this.scopeValidator.validateDiff(diff, task.allowedPaths);
    if (!scopeResult.passed) throw new ScopeViolationError(scopeResult);

    // 3. 密钥扫描（Layer 6）
    const secretResult = await this.secretScanner.scan(diff);
    if (secretResult.found) throw new SecretFoundError(secretResult);

    // 4. git add（只添加 allowedPaths 内的文件）
    for (const file of diff.files) {
      if (scopeResult.allowedFiles.has(file.path)) {
        await git.add(workspace.worktreePath, file.path);
      }
    }

    // 5. git commit
    const message = [
      `harness: ${task.id} - ${task.goal}`,
      ``,
      `Task: ${task.id}`,
      `Project: ${task.projectId}`,
      `Agent: ${task.assignedProfileId}`,
      `Goal Contract: v${task.goalContractVersion}`,
      `Plan: v${task.planVersion}`,
    ].join('\n');

    const commitHash = await git.commit(workspace.worktreePath, message);

    // 6. 验证 commit 非空
    if (!commitHash) throw new Error('Commit created no changes');

    // 7. 记录审计
    await this.auditLogger.log('CommitCreated', { taskId: task.id, commitHash });

    return commitHash;
  }
}
```

### 4.2 Merge 流程

```typescript
class GitMergeService {
  async mergeTask(task: Task): Promise<MergeResult> {
    const targetBranch = `integration/${task.projectId}`;

    // 1. 切换到集成分支
    await git.checkout(targetBranch);

    // 2. Cherry-pick 任务分支的 commit
    try {
      await git.cherryPick(task.commitHash);
    } catch (conflict) {
      return { status: "conflict", conflictFiles: parseConflictFiles(conflict) };
    }

    // 3. 运行集成测试（如配置）
    if (task.acceptanceChecks.length > 0) {
      const result = await this.verificationService.runChecks(task.acceptanceChecks);
      if (!result.passed) {
        // 回滚 cherry-pick
        await git.cherryPickAbort();
        return { status: "integration_test_failed", failures: result.failures };
      }
    }

    return { status: "merged" };
  }
}
```

---

## 5. Worker Agent 的 Git 限制

### 5.1 默认禁止的命令

```
git commit
git push
git push --force
git reset --hard
git rebase
git checkout (切换到其他分支)
git merge
git branch -D (强制删除分支)
git tag
```

### 5.2 默认允许的命令（只读）

```
git status
git diff
git log
git branch (只读列表)
git stash (允许暂存本地修改)
```

### 5.3 例外申请

如果任务确实需要 git 操作（如 monorepo 发布工具），可以在 TaskEnvelope 中声明：

```json
{
  "scope": {
    "allowedGitCommands": ["git add", "git commit -m"],
    "allowedPaths": ["packages/my-package/**"]
  }
}
```

例外情况记录在 audit_log。

---

## 6. Worktree 粒度策略

| 策略 | 适用场景 | Foundation 是否支持 |
|------|---------|:---:|
| 一任务一 worktree | 独立修改、低耦合 | ✅ 默认 |
| 一工作流一 worktree | 紧密耦合的多个串行任务 | ❌ 后续版本 |
| 串行共用 worktree | 高冲突区域（共享模块修改） | ❌ 后续版本 |

Foundation Release **默认一任务一 worktree**，Scheduler 不做并行冲突判断（串行执行所有 READY 任务）。

---

## 7. 架构变更时的 Worktree 处理

当 Plan Revision 导致架构变更时：

1. 生成 Change Request
2. 识别受影响的正在执行/已完成的 Task
3. 已完成的任务：如果分支与变更后的架构兼容 → 保留；否则 → 标记 SUPERSEDED
4. 正在执行的任务：暂停 → 等待架构迁移任务完成 → Rebase 到新基线 → 恢复
5. 未开始的任务：在 DAG 更新后按新架构执行

---

## 8. WorkspaceLease 机制

```typescript
interface WorkspaceLease {
  leaseId: string;
  taskId: string;
  worktreePath: string;
  branchName: string;
  acquiredAt: string;
  expiresAt: string;           // 租约过期时间（防死锁）
  renewedAt: string;           // 最近续约时间
  status: "active" | "expired" | "released";
}

class WorkspaceLeaseService {
  /** 获取租约（如果 worktree 已被占用则失败） */
  async acquire(taskId: string, worktreePath: string): Promise<WorkspaceLease>;

  /** 续约 */
  async renew(leaseId: string): Promise<void>;

  /** 释放 */
  async release(leaseId: string): Promise<void>;

  /** 检测过期租约（reconciliation 时使用） */
  async findExpiredLeases(): Promise<WorkspaceLease[]>;
}
```

- 租约防止两个任务使用同一个 worktree
- 租约到期 → reconciliation 时自动释放（假设持有者已死）
- 租约存储在 SQLite 中
