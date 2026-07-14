# Foundation Release Implementation Plan — Agent Harness

> **版本**: v1.0
> **日期**: 2026-07-14
> **预计总工期**: 8-12 周

---

## 阶段总览

```
F0  →  F1  →  F2  →  F3  →  F4  →  F5  →  F6  →  F7  →  F8  →  F9  →  F10
│       │       │       │       │       │       │       │       │       │
│       │       │       │       │       │       │       │       │       └─ 文档+验收
│       │       │       │       │       │       │       │       └─ Golden Path
│       │       │       │       │       │       │       └─ CLI
│       │       │       │       │       │       └─ Scheduler+DAG+Verification
│       │       │       │       │       └─ Discovery+Probe
│       │       │       │       └─ Codex+Claude Adapters
│       │       │       └─ FakeAdapter+Contract Tests
│       │       └─ Process+Policy+Workspace+Git
│       └─ Event Store+Persistence+State Machine
└─ Repo+Toolchain
```

---

## F0: Repository, Toolchain, Quality Gates

**目标**：可工作的开发环境

### Tasks
- [ ] 初始化 TypeScript 项目 (Node.js 18+, ESM)
- [ ] 配置 ESLint + Prettier
- [ ] 配置 Vitest + coverage thresholds
- [ ] 配置 import/no-cycle lint rule
- [ ] 创建目录结构 (`src/domain/`, `src/application/`, `src/infrastructure/`, `src/cli/`, `src/testing-kit/`)
- [ ] 配置 `tsconfig.json` + project references (如有需要)
- [ ] 创建空的 SQLite migration 框架
- [ ] 配置 CI (GitHub Actions: lint + typecheck + test)
- [ ] 创建 `package.json` scripts

**输出物**：可编译运行的空项目骨架

---

## F1: Domain Contracts & Dependency Rules

**目标**：所有核心接口和类型定义冻结

### Tasks
- [ ] 定义所有 `domain/contracts/` 类型 (参见 `adapter-contract.md` 中的清单)
- [ ] 定义 Zod Schema（RuntimeProfile、TaskEnvelope、TaskResult 等）
- [ ] 编写 `domain/policies/` 策略类型
- [ ] 编写依赖规则文档 `dependency-rules.md`
- [ ] 配置 import 限制规则 (ESLint no-restricted-imports)
- [ ] 编写循环依赖检测脚本

**输出物**：完整的类型系统 + 依赖规则强制执行

---

## F2: Event Store, Persistence, State Machine

**目标**：完整的事件存储和状态机

### Tasks
- [ ] 实现 SQLite connection manager (WAL mode)
- [ ] 实现 `event-store.ts` (append + query by stream)
- [ ] 实现 `projections.ts` (rebuild from events)
- [ ] 创建 migration v1 (event_log, projections, audit_log, agent_events)
- [ ] 实现 `project-fsm.ts` (21 状态 + 完整转换表)
- [ ] 实现 `task-fsm.ts` (17 状态 + 完整转换表)
- [ ] 实现 `transition-service.ts` (含幂等、前置条件检查)
- [ ] 单元测试：所有合法转换
- [ ] 单元测试：所有非法转换被拒绝
- [ ] 单元测试：幂等性

**输出物**：完整的持久化层 + 状态机

---

## F3: Process, Policy, Workspace, Git

**目标**：基础设施层核心模块

### Tasks
- [ ] 实现 `ProcessManager.ts` (spawn, terminate, kill, isAlive)
- [ ] 实现 `cancellation.ts` (timeout, AbortController propagation)
- [ ] 实现 `CommandPolicyEngine.ts` (拦截 + 审批模式)
- [ ] 实现 `FileScopeValidator.ts` (路径规范化 + 逃逸检测)
- [ ] 实现 `SecretScanner.ts` (密钥模式匹配)
- [ ] 实现 `GitInspector.ts` (git status/diff/log 查询)
- [ ] 实现 `WorktreeManager.ts` (create, list, cleanup)
- [ ] 实现 `WorkspaceLease.ts` (acquire, renew, release, expire)
- [ ] 集成测试：worktree 创建/清理
- [ ] 集成测试：命令拦截

**输出物**：进程/策略/工作区/Git 全套基础设施

---

## F4: Agent Runtime & Fake Adapter

**目标**：AgentAdapter 契约 + FakeAdapter + Contract Test Kit

### Tasks
- [ ] 固化 `AgentAdapter` 接口
- [ ] 实现 `FakeAgentAdapter` (完整生命周期 + 可脚本化)
- [ ] 实现 `AgentSession` 生命周期管理
- [ ] 实现 `testing-kit/AdapterContractTest.ts`
- [ ] 实现 `testing-kit/FakeAgentFactory.ts`
- [ ] 实现 `testing-kit/TestFixtures.ts`
- [ ] FakeAdapter 通过完整 contract test suite

**输出物**：稳定的 Adapter 契约 + 可复用的契约测试套件

---

## F5: Codex & Claude Adapters

**目标**：真实 Agent Adapter

### Tasks
- [ ] 实现 `CodexSdkAdapter` (子进程 + SDK)
- [ ] 实现 `ClaudeCliAdapter` (stream-json 子进程)
- [ ] 两个 Adapter 通过 contract test suite
- [ ] Claude stream-json 协议解析测试
- [ ] 集成测试（需要 Agent 已安装和认证）

**输出物**：两个可用的真实 Agent Adapter

---

## F6: Discovery, Runtime Profile, Probe

**目标**：Agent 自动发现和能力探测

### Tasks
- [ ] 实现 `AgentDiscoveryService` (扫描 + identify)
- [ ] 实现 `probe()` 流程 (临时仓库微型任务)
- [ ] 实现 Profile 状态管理 (DETECTED → ... → AVAILABLE)
- [ ] 实现配置加载 `~/.harness/config.json`
- [ ] CLI: `harness config` 命令
- [ ] CLI: `harness agents` 命令

**输出物**：Agent 发现和 Profile 管理完整流程

---

## F7: Scheduler, DAG, Verification, Commit

**目标**：任务调度、验收和 Harness 控制的 Git 操作

### Tasks
- [ ] 实现 DAG 拓扑排序
- [ ] 实现 `scheduler.ts` (READY 判定 + 串行执行)
- [ ] 实现 `verification.ts` (acceptanceChecks 执行)
- [ ] 实现 `diff-inspection.ts` (任务前后 diff 检查)
- [ ] 实现 `git-commit-service.ts` (Harness 独占 commit)
- [ ] 实现 `git-merge-service.ts` (Cherry-pick + 集成)
- [ ] 实现 `FakePlanningProvider` (返回手工 Task DAG)
- [ ] 集成测试：完整 task 执行流程 (FakeAdapter)

**输出物**：从 DAG 定义到 commit 的完整执行链路

---

## F8: CLI & Application Facade

**目标**：用户可用的命令行工具

### Tasks
- [ ] 实现 `HarnessApi` (application facade)
- [ ] 实现 CLI 入口 `main.ts` (命令路由)
- [ ] 实现 `harness run` (创建 project → 审批 → 执行)
- [ ] 实现 `harness status` (查询当前状态)
- [ ] 实现 `harness approve` (批准计划)
- [ ] 实现 `harness cancel` (取消运行)
- [ ] 实现 `harness resume` (恢复运行)
- [ ] 终端输出格式化 (表格、颜色编码)

**输出物**：完整的命令行工具

---

## F9: Golden Path, Recovery, Contract Tests

**目标**：端到端可运行 + 崩溃恢复

### Tasks
- [ ] 实现 `reconciliation.ts` (启动时恢复流程)
- [ ] 实现 `checkpoint-store.ts` (保存/加载检查点)
- [ ] 实现 Golden Path E2E 测试 (FakeAdapter)
- [ ] 实现 Crash Recovery 测试 (FakeAdapter)
- [ ] 实现 Agent Unavailable 行为测试
- [ ] 确保所有 Adapter 通过 contract test suite
- [ ] CLI integration tests

**输出物**：Golden Path 可在 CI 中始终运行

---

## F10: Documentation, Risk Review, Foundation Acceptance

**目标**：Foundation Release 验收

### Tasks
- [ ] 完成所有规划文档终稿
- [ ] 风险审查报告
- [ ] 需求追溯矩阵
- [ ] 运行 Foundation Acceptance Criteria
- [ ] 修复关键缺陷
- [ ] 发布 Foundation Release tag

**输出物**：Foundation Release 正式发布

---

## 阶段依赖图

```
F0 ──→ F1 ──→ F2 ──→ F7 ──→ F8 ──→ F9 ──→ F10
         │              │
         └──→ F3 ──────┘
         │
         └──→ F4 ──→ F5 ──→ F6 ──→ F7
```

- F2 和 F3 不互相依赖，可以并行
- F4 不依赖 F2/F3，可以独立启动
- F5 依赖 F4（Adapter 契约）
- F6 依赖 F5（需要真实 Adapter 做探测）
- F7 汇集 F2、F3、F6 的产出
