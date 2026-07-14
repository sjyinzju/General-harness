# ADR-003: Git Operations Owned by Harness

> **状态**: Accepted
> **日期**: 2026-07-14

---

## Context

Worker Agent（Claude Code、Codex 等）在执行代码任务时可能需要 git 操作。需要决定谁拥有 Git 操作权限。

## Decision

**Harness 独占所有 Git 写操作**。Worker Agent 默认不得执行 `git commit`、`git push`、`git reset --hard`、`git rebase` 等写操作。

## Rationale

1. **安全**：Agent 可能被提示注入攻击，git push --force 可能造成不可逆损害
2. **审计**：所有 commit 必须由 Harness 创建，附带 taskId/projectId 元数据
3. **幂等**：Harness 可以验证 commit 是否已存在，避免重复
4. **验证门**：commit 之前强制执行密钥扫描、文件范围验证
5. **一致性**：commit message 格式统一，便于追溯

## Consequences

- **正面**：安全边界清晰，Agent 无法篡改 Git 历史
- **正面**：所有 commit 携带完整的审计元数据
- **正面**：commit 前强制安全扫描
- **负面**：如果 Agent 确实需要做 git 操作（如 monorepo 发布），需要额外声明
- **负面**：Harness 必须实现完整的 Git 操作封装

## Exception Path

如果任务确实需要 git 操作，可以在 `TaskEnvelope.scope.allowedGitCommands` 中声明。例外情况记录在 audit_log。
