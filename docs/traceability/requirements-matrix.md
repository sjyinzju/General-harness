# Requirements Traceability Matrix — Agent Harness Foundation Release

> **版本**: v1.0
> **日期**: 2026-07-14

---

## 概述

此矩阵将 Foundation Release 的 32 项必须实现与验收标准、Golden Path 步骤和测试进行追溯。

| ID | 需求 | 类型 | 验收标准 | 测试覆盖 | 阶段 |
|----|------|------|---------|---------|------|
| REQ-001 | CLI 基础命令 | MUST | AC-10 | CLI integration tests | F8 |
| REQ-002 | 配置加载和校验 | MUST | AC-10 | Unit test (config schema) | F6 |
| REQ-003 | SQLite migration 框架 | MUST | AC-3 | Unit test (migration runner) | F2 |
| REQ-004 | Append-only event store | MUST | AC-3 | Unit test (event store CRUD) | F2 |
| REQ-005 | State projection | MUST | AC-3 | Unit test (rebuild projection) | F2 |
| REQ-006 | 项目双层状态机 | MUST | AC-2 | Exhaustive transition tests | F2 |
| REQ-007 | 任务双层状态机 | MUST | AC-2 | Exhaustive transition tests | F2 |
| REQ-008 | Command handler | MUST | AC-2, AC-3 | Unit test (each handler) | F2 |
| REQ-009 | AgentAdapter 契约 | MUST | AC-4 | Contract test suite | F4 |
| REQ-010 | FakeAgentAdapter | MUST | AC-4, AC-9 | Contract tests + Golden Path | F4 |
| REQ-011 | CodexSdkAdapter | MUST | AC-4, AC-13 | Contract tests (conditional) | F5 |
| REQ-012 | ClaudeCliAdapter | MUST | AC-4, AC-13 | Contract tests (conditional) | F5 |
| REQ-013 | AgentDiscoveryService | MUST | AC-10 (`harness agents`) | Unit test + Integration | F6 |
| REQ-014 | Runtime Profile probe | MUST | AC-4 (probe in contract) | Contract test T5 | F6 |
| REQ-015 | Git repository inspection | MUST | AC-5 | Unit test (GitInspector) | F3 |
| REQ-016 | WorktreeManager | MUST | AC-5 | Integration test | F3 |
| REQ-017 | WorkspaceLease | MUST | AC-5, AC-8 | Integration test | F3 |
| REQ-018 | ProcessManager | MUST | AC-6 | Integration test | F3 |
| REQ-019 | Cancellation/timeout | MUST | AC-6 | Integration test | F3 |
| REQ-020 | DAG 基础模型 | MUST | AC-9 | Unit test (topological sort) | F7 |
| REQ-021 | 确定性 Scheduler | MUST | AC-9 | Unit test + Golden Path | F7 |
| REQ-022 | Policy engine | MUST | AC-7 | Unit test (each policy) | F3 |
| REQ-023 | Command execution record | MUST | AC-6, AC-7 | Audit log test | F3 |
| REQ-024 | File scope validation | MUST | AC-5, AC-7 | Unit test (path validation) | F3 |
| REQ-025 | Diff inspection | MUST | AC-5 | Unit test | F7 |
| REQ-026 | 确定性 verification | MUST | AC-9 | Unit test + Golden Path | F7 |
| REQ-027 | Harness-owned commit | MUST | AC-5 | Integration test | F7 |
| REQ-028 | Checkpoint | MUST | AC-8 | Unit test + Crash test | F9 |
| REQ-029 | Crash reconciliation | MUST | AC-8 | Crash recovery E2E | F9 |
| REQ-030 | Structured logging | SHOULD | AC-11 | Manual inspection | F0 |
| REQ-031 | Adapter contract test kit | MUST | AC-4 | Self-testing | F4 |
| REQ-032 | CLI integration tests | MUST | AC-10 | Integration test | F8 |

---

## Golden Path 覆盖

| Golden Path 步骤 | 涉及 REQ | 
|-----------------|----------|
| 创建 Project | REQ-001, REQ-008 |
| 创建 Goal Contract | REQ-008 |
| 创建 Plan | REQ-008 |
| 创建 Task | REQ-006, REQ-007 |
| Scheduler 判定 READY | REQ-021 |
| 分配 Runtime Profile | REQ-013, REQ-014 |
| 创建 WorkspaceLease | REQ-017 |
| 创建 Git worktree | REQ-016 |
| 调用 AgentAdapter | REQ-009, REQ-010 |
| 接收 AgentEvent | REQ-010 |
| Agent 修改文件 | REQ-025 |
| 收集 Git diff | REQ-015, REQ-025 |
| 检查 allowed paths | REQ-024 |
| 执行 acceptance checks | REQ-026 |
| 生成 VerificationEvidence | REQ-026 |
| Harness 创建 commit | REQ-027 |
| 更新 Task 状态 | REQ-006, REQ-007 |
| 更新 Project 状态 | REQ-006 |
| 关闭进程 | REQ-018, REQ-019 |
| 重启 Harness | REQ-029 |
| 从 SQLite 恢复 | REQ-004, REQ-005 |
| 查询最终结果 | REQ-001 |

---

## 安全需求追溯

| 安全需求 | 实现层 | REQ |
|---------|--------|-----|
| 工具白名单 | Layer 1 | REQ-022 |
| 子进程隔离 | Layer 2 | REQ-016, REQ-018 |
| 命令拦截 | Layer 3 | REQ-022, REQ-023 |
| 路径逃逸防护 | Layer 4 | REQ-024 |
| Diff 检查 | Layer 5 | REQ-025 |
| 密钥扫描 | Layer 6 | REQ-022 |
| 回滚/废弃 | Layer 7 | AC-5 |
| API Key 不存储 | Auth | REQ-013 (inspectConfig 不读密钥) |
