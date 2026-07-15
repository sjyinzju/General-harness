# Foundation Acceptance Criteria v3

> **版本**: v3.0
> **日期**: 2026-07-15
> **修订**: 四套生命周期, Operation/Saga, Resource Claim, AgentEvent v2, Gate 对齐

---

## AC-1: Architecture (MUST)
- [ ] Cargo workspace 4 crates 编译通过
- [ ] harness-core 无 rusqlite/tokio/git2/ratatui 依赖
- [ ] 无循环依赖
- [ ] 四套核心生命周期全部实现

## AC-2: State Machines (MUST)
- [ ] Project/Task/ExecutionAttempt/WorkspaceLease FSM
- [ ] 合法转换穷举测试
- [ ] 非法转换拒绝测试
- [ ] 辅助维度独立修改
- [ ] Idempotency key 唯一约束

## AC-3: Operation/Saga (MUST)
- [ ] 所有 Git 副作用进入 Operation 模型
- [ ] Phase 1 → 2 → 3 流程
- [ ] Git commit trailer 含 operation_id
- [ ] Reconciliation 处理 PENDING/RUNNING operations

## AC-4: Agent Adapters (MUST)
- [ ] FakeAdapter 通过 contract tests
- [ ] ClaudeCliAdapter 通过 contract tests (条件)
- [ ] CodexCliAdapter 通过 contract tests (条件)
- [ ] AgentEvent v2: Progress+ProcessExited+RawVendorEvent
- [ ] SessionEnd 正确标记 synthetic/abnormal

## AC-5: Resource Claim (MUST)
- [ ] File/directory/repo/logical 四种类型
- [ ] READ/READ 兼容, WRITE/✗ 冲突
- [ ] 与 profile slot + workspace lease 原子检查

## AC-6: Git (MUST)
- [ ] 禁止空 commit
- [ ] 无 diff 按 Task 类型处理
- [ ] 禁止自动切换合并策略
- [ ] IntegrationJob + IntegrationRepairTask
- [ ] Orphan worktree 验证 ownership marker + namespace + operation_id

## AC-7: Process (MUST)
- [ ] Headless supervisor 独立进程
- [ ] IPC client (CLI) 通过 socket 通信
- [ ] Watchdog 终止 Agent 子进程树
- [ ] Supervisor 崩溃 → Execution LOST
- [ ] LOST → 新 Execution + --resume

## AC-8: Recovery (MUST)
- [ ] Reconciliation 检测 ORPHANED 和 LOST
- [ ] Operation reconciliation (PENDING/RUNNING → 检查完成证据)
- [ ] 并发 execution 独立恢复

## AC-9: CLI (MUST)
- [ ] 交互式 TUI (ratatui) + non-interactive mode
- [ ] --approve flag 显式传入
- [ ] CI no-TTY Golden Path

## AC-10: Golden Path (MUST)
- [ ] FakeAdapter: 两任务并行成功
- [ ] FakeAdapter: 一成功一失败 + 下游 SUPERSEDED
- [ ] FakeAdapter: 取消不影响另一任务
- [ ] FakeAdapter: 崩溃后 LOST → retry --resume
- [ ] FakeAdapter: Resource Claim 冲突 → 串行
