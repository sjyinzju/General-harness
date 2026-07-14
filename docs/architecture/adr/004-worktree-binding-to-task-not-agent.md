# ADR-004: Worktree Binding to Task, Not Agent

> **状态**: Accepted
> **日期**: 2026-07-14

---

## Context

Worktree 是 Agent 执行任务的隔离工作区。需要决定 worktree 如何命名和组织。

## Decision

**Worktree 绑定 Task 或 Workstream，不绑定 Agent 或 Model。**

命名格式：`.harness/worktrees/{taskId}-{short-description}/`

Agent 和 Model 信息存储在 SQLite 数据库中，不写入目录名称。

## Rationale

1. **Agent 可更换**：如果 Codex 失败，可以换 Claude 接管同一 worktree
2. **目录稳定性**：Agent 更换时 worktree 路径不变
3. **数据正规化**：Agent 分派历史是多对多的（一任务可能被多个 Agent 尝试），存入关系型数据库
4. **可读性**：目录名简洁，与项目逻辑结构对齐

## Consequences

- **正面**：任务可以在不同 Agent 间无缝转移
- **正面**：目录命名简洁稳定
- **负面**：需要查数据库才能知道"谁在执行这个任务"（通过 CLI 或日志可以查看）

## Rejected Alternative

将 Agent 和 Model 放入目录名（如 `TASK-014-auth-callback-codex-gpt4/`）：
- Agent 换人时需要重命名目录
- 目录名字过长
- Agent 信息与目录生命周期耦合
