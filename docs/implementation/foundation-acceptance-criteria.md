# Foundation Release Acceptance Criteria — Agent Harness

> **版本**: v1.0
> **日期**: 2026-07-14

---

## 验收总则

Foundation Release 验收通过的条件：**所有 MUST 项通过 + 至少 80% 的 SHOULD 项通过**。

---

## AC-1: Domain & Contracts (MUST)

- [ ] 所有 `domain/contracts/` 接口有 Zod Schema 校验
- [ ] Runtime Profile Schema 包含所有必需字段
- [ ] TaskEnvelope/TaskResult Schema 定义且可校验
- [ ] 所有跨模块数据经过 Schema 校验
- [ ] 无 `any` 类型绕过检查

## AC-2: State Machine (MUST)

- [ ] 项目 21 状态全部实现
- [ ] 任务 17 状态全部实现
- [ ] 所有合法转换可通过 TransitionService 执行
- [ ] 每种非法转换类型至少有一例被正确拒绝
- [ ] 所有转换支持 idempotencyKey 幂等
- [ ] 业务代码不直接修改状态字段

## AC-3: Event Store (MUST)

- [ ] SQLite WAL mode enabled
- [ ] Append-only event_log 表
- [ ] 可以从 event_log 完全重建 projection
- [ ] 每个 event 有唯一 idempotency_key
- [ ] 命令和事件在同一事务中
- [ ] Migration runner 可执行

## AC-4: Agent Adapter Contract (MUST)

- [ ] FakeAgentAdapter 通过完整 contract test suite
- [ ] CodexSdkAdapter 通过完整 contract test suite（需要 Codex 环境）
- [ ] ClaudeCliAdapter 通过完整 contract test suite（需要 Claude 环境）
- [ ] Contract test suite 可被新 Adapter 复用

## AC-5: Git & Workspace (MUST)

- [ ] Worktree 命名仅为 `{taskId}-{short-description}`（无 Agent/Model）
- [ ] Harness 独占 git add + commit
- [ ] Worker Agent 默认不可执行 git 写操作
- [ ] 密钥扫描在 commit 前执行
- [ ] 文件范围检查在 commit 前执行
- [ ] Worktree 在任务完成后可正常清理

## AC-6: Process Management (MUST)

- [ ] 子进程可正常启动和终止
- [ ] 超时后子进程被强制终止
- [ ] 取消传播到 SIGTERM → SIGKILL
- [ ] 孤儿进程被 reconciliation 检测
- [ ] 所有命令执行被审计记录

## AC-7: Safety (MUST)

- [ ] 危险命令模式被拦截
- [ ] 文件路径逃逸被阻止
- [ ] `.git/` 和 `.harness/` 目录被保护
- [ ] API Key 不被读取或存储
- [ ] 所有安全违规记录在 audit_log

## AC-8: Recovery (MUST)

- [ ] 进程崩溃后可从 SQLite 恢复状态
- [ ] ORPHANED 任务可自动重试
- [ ] retryCount 达上限后任务标记为 FAILED_TERMINAL
- [ ] 启动时 reconciliation 可正确识别所有非终端状态任务

## AC-9: Golden Path (MUST)

- [ ] FakeAdapter Golden Path E2E 测试在 CI 中始终通过
- [ ] 覆盖：创建 Project → Goal Contract → Plan → Task DAG → Dispatch → Execute → Verify → Commit → Done

## AC-10: CLI (MUST)

- [ ] `harness config` 可交互式配置
- [ ] `harness run` 可启动 Golden Path
- [ ] `harness status` 显示项目/任务状态
- [ ] `harness cancel` 可取消运行中的项目
- [ ] 所有命令有合理的错误输出

## AC-11: Code Quality (SHOULD)

- [ ] 无 ESLint 错误
- [ ] 无 TypeScript 错误
- [ ] Domain 层单元测试覆盖率 > 80%
- [ ] 无循环依赖
- [ ] 无未使用的抽象（0 个调用者的 export）
- [ ] 无空壳 package

## AC-12: Documentation (SHOULD)

- [ ] 所有架构文档完成
- [ ] README 包含快速开始指南
- [ ] ADR 记录所有关键决策
- [ ] 风险登记册完成
- [ ] 需求追溯矩阵完成

## AC-13: Adapter Coverage (SHOULD)

- [ ] CodexSdkAdapter Integration Path 在环境可用时通过
- [ ] ClaudeCliAdapter Integration Path 在环境可用时通过
- [ ] Agent 不可用时显示清晰的 DEGRADED/UNAVAILABLE 信息

---

## 验收流程

```
1. 运行: npm run test:all
2. 运行: npm run lint && npm run typecheck
3. 运行: npm run test:e2e:golden-path (FakeAdapter)
4. 手动运行: npm run test:e2e:codex (需要 Codex 环境)
5. 手动运行: npm run test:e2e:claude (需要 Claude 环境)
6. 检查 coverage report
7. 检查 dependency graph (无循环)
8. 检查未使用的 export (0)
9. 审查文档完整性
10. 签名: Foundation Release Accepted
```
