# Requirements Traceability Matrix v3

> **版本**: v3.0
> **日期**: 2026-07-15
> **修订**: 匹配 v3 四套生命周期 + Operation/Saga + Resource Claim

---

| ID | 需求 | Gate | AC | 测试 |
|----|------|------|----|------|
| REQ-001 | Cargo workspace 4 crates + 单二进制 | A | AC-1 | cargo build |
| REQ-002 | harness-core 零外部依赖 | A | AC-1 | cargo check + dependency guard |
| REQ-003 | 四套核心生命周期 FSM | A | AC-2 | 穷举测试 |
| REQ-004 | Execution Attempt 状态 (含 LOST) | A | AC-2 | FSM 测试 |
| REQ-005 | Workspace Lease 状态 | A | AC-2 | FSM 测试 |
| REQ-006 | Operation/Saga 三阶段 | B | AC-3 | 单元 + 集成 |
| REQ-007 | Git trailer operation_id | B | AC-3 | 集成测试 |
| REQ-008 | Operation reconciliation | D | AC-3, AC-8 | Recovery 测试 |
| REQ-009 | AgentAdapter trait | B | AC-4 | Contract tests |
| REQ-010 | AgentEvent v2 (Progress/ProcessExited/RawVendorEvent) | B | AC-4 | Contract tests |
| REQ-011 | SessionEnd synthetic + abnormal 标记 | B | AC-4 | Contract tests |
| REQ-012 | FakeAgentAdapter | B | AC-4 | Golden Path |
| REQ-013 | CodexCliAdapter | C | AC-4 | Contract tests (conditional) |
| REQ-014 | ClaudeCliAdapter | C | AC-4 | Contract tests (conditional) |
| REQ-015 | Resource Claim (file/dir/repo/logical) | D | AC-5 | 单元 + Golden Path |
| REQ-016 | READ/READ 兼容, WRITE conflict | D | AC-5 | Golden Path |
| REQ-017 | Profile slot + lease + claims 原子检查 | D | AC-5 | 集成测试 |
| REQ-018 | 禁止空 commit | D | AC-6 | 单元测试 |
| REQ-019 | 无 diff 按 Task 类型处理 | D | AC-6 | 集成测试 |
| REQ-020 | 禁止自动切换合并策略 | D | AC-6 | 单元测试 |
| REQ-021 | IntegrationJob + IntegrationRepairTask | D | AC-6 | Golden Path |
| REQ-022 | Orphan worktree 三项验证 | D | AC-6 | Recovery 测试 |
| REQ-023 | Headless supervisor + IPC client | D | AC-7 | 集成测试 |
| REQ-024 | Watchdog Agent 终止 | D | AC-7 | Recovery 测试 |
| REQ-025 | Supervisor crash → LOST → --resume | D | AC-7 | Recovery E2E |
| REQ-026 | Interactive CLI (ratatui) + non-interactive | D | AC-9 | CLI test |
| REQ-027 | --approve 显式传入 | D | AC-9 | CLI test |
| REQ-028 | CI no-TTY Golden Path | D | AC-9 | CI test |
| REQ-029 | 两任务并行成功 | D | AC-10 | Golden Path |
| REQ-030 | 一成功一失败 + 下游 SUPERSEDED | D | AC-10 | Golden Path |
| REQ-031 | 取消不影响另一 | D | AC-10 | Golden Path |
| REQ-032 | Resource Claim 冲突 → 串行 | D | AC-10 | Golden Path |
| REQ-033 | 崩溃后 LOST → retry | D | AC-10 | Golden Path |
| REQ-034 | Tech spike: Codex + Claude 事件映射验证 | B→C | — | Spike report |
| REQ-035 | Agent unavailable → clear DEGRADED message | D | AC-10 | Golden Path |
