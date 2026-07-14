# Container & Module Design — Agent Harness

> **文档类型**: 架构文档 (C4 Level 2)
> **版本**: v1.0
> **日期**: 2026-07-14

---

## 1. 设计目标

- **长期可维护**：模块边界清晰，依赖方向严格单向
- **可测试**：核心逻辑不依赖外部进程、文件系统或数据库
- **可扩展**：新 Agent Adapter 可以通过实现契约接口添加
- **单一进程**：Foundation Release 整个 Harness 作为单个 Node.js 进程运行

---

## 2. 容器视图

```
┌─────────────────────────────────────────────────────────────┐
│                     Agent Harness Process                    │
│                      (Single Node.js)                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │                    CLI / Local API                     │ │
│  │  (用户界面层 - 唯一的入口点)                             │ │
│  └────────────────────────┬──────────────────────────────┘ │
│                           │                                 │
│  ┌────────────────────────▼──────────────────────────────┐ │
│  │               Application Layer                        │ │
│  │  ┌──────────┐ ┌──────────┐ ┌───────────────────────┐ │ │
│  │  │ Command   │ │ Services │ │ Orchestration          │ │ │
│  │  │ Handlers  │ │(Scheduler│ │ (FakePlanningProvider) │ │ │
│  │  │           │ │ Verifier │ │                        │ │ │
│  │  └──────────┘ └──────────┘ └───────────────────────┘ │ │
│  └────────────────────────┬──────────────────────────────┘ │
│                           │                                 │
│  ┌────────────────────────▼──────────────────────────────┐ │
│  │               Domain Layer                             │ │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │ │
│  │  │ Contracts    │ │ State Machine│ │ Policies     │  │ │
│  │  │ (接口+类型)   │ │ (21+17状态)  │ │(类型定义)     │  │ │
│  │  └──────────────┘ └──────────────┘ └──────────────┘  │ │
│  └────────────────────────┬──────────────────────────────┘ │
│                           │                                 │
│  ┌────────────────────────▼──────────────────────────────┐ │
│  │               Infrastructure Layer                     │ │
│  │  ┌──────────┐ ┌────────────┐ ┌────────────────────┐  │ │
│  │  │Persistence│ │ Adapters   │ │ Workspace + Process│  │ │
│  │  │(SQLite)  │ │(Fake/Codex/│ │(Worktree, Lease,   │  │ │
│  │  │          │ │ Claude CLI)│ │ ProcessManager)    │  │ │
│  │  └──────────┘ └────────────┘ └────────────────────┘  │ │
│  │  ┌──────────┐ ┌────────────┐ ┌────────────────────┐  │ │
│  │  │Policy    │ │ Discovery  │ │ Logging            │  │ │
│  │  │Engine    │ │(Agent      │ │(StructuredLogger)  │  │ │
│  │  │          │ │ Discovery) │ │                    │  │ │
│  │  └──────────┘ └────────────┘ └────────────────────┘  │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │                  Testing Kit                           │ │
│  │  (AdapterContractTest, FakeAgentFactory, TestFixtures) │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. 模块详细设计

### 3.1 Domain Layer (`src/domain/`)

**不依赖任何外部系统或库（仅 TypeScript + Zod）**

```
domain/
├── contracts/                    # 核心接口和类型
│   ├── AgentAdapter.ts           # AgentAdapter 接口
│   ├── AgentIdentity.ts          # AgentIdentity, AgentKind
│   ├── RuntimeProfile.ts         # RuntimeProfile, CapabilitySet, ProbeResult
│   ├── TaskEnvelope.ts           # TaskEnvelope, FileScope
│   ├── TaskResult.ts             # TaskResult (Worker 返回的声明)
│   ├── GoalContract.ts           # GoalContractVersion, ChangeRequest
│   ├── Project.ts                # Project, ProjectStatus
│   ├── Task.ts                   # Task, TaskStatus, TaskDependency
│   ├── Workspace.ts              # Workspace, WorkspaceLease
│   ├── Artifact.ts               # Artifact, ArtifactReference
│   ├── DomainEvent.ts            # DomainEvent, EventEnvelope, Command
│   ├── AgentEvent.ts             # AgentEvent, AgentToolEvent
│   ├── AcceptanceCheck.ts        # AcceptanceCheck
│   ├── VerificationEvidence.ts   # VerificationEvidence
│   ├── ApprovalRequest.ts        # ApprovalRequest
│   ├── Checkpoint.ts             # Checkpoint
│   └── index.ts                  # barrel export

├── state-machine/                # 纯函数状态机
│   ├── project-fsm.ts            # 项目级 FSM
│   ├── task-fsm.ts               # 任务级 FSM
│   └── transition-rules.ts       # 转换规则定义

└── policies/                     # 策略类型定义
    ├── BudgetPolicy.ts
    ├── PermissionPolicy.ts
    ├── CommandPolicy.ts
    └── FileScopePolicy.ts
```

### 3.2 Application Layer (`src/application/`)

**依赖 domain/contracts、domain/state-machine、domain/policies**

```
application/
├── commands/                     # Command Handler (每个 command 一个文件)
│   ├── create-project.ts
│   ├── create-goal-contract.ts
│   ├── approve-plan.ts
│   ├── dispatch-task.ts
│   ├── submit-task-result.ts
│   ├── verify-task.ts
│   ├── commit-task.ts
│   ├── merge-task.ts
│   └── cancel-project.ts

├── services/                     # 应用服务
│   ├── transition-service.ts     # 唯一的状态转换入口
│   ├── scheduler.ts              # DAG 拓扑排序 + 就绪任务判定
│   ├── verification.ts           # 验收检查执行
│   ├── reconciliation.ts         # 崩溃恢复协调
│   └── event-publisher.ts        # 事件发布（内存）

└── orchestration/                # 编排（Foundation: 简单确定性实现）
    └── fake-planning-provider.ts  # 返回手工 Task DAG
```

### 3.3 Infrastructure Layer (`src/infrastructure/`)

**依赖 application + domain**

```
infrastructure/
├── persistence/
│   ├── sqlite/
│   │   ├── connection.ts         # SQLite 连接管理
│   │   ├── event-store.ts        # Append-only event log
│   │   ├── projections.ts        # Current state projection
│   │   ├── audit-store.ts        # 审计日志
│   │   ├── agent-event-store.ts  # Agent 运行时事件
│   │   └── migrations/
│   │       ├── 001_initial.ts
│   │       └── index.ts          # Migration runner
│   └── file/
│       ├── checkpoint-store.ts
│       ├── artifact-store.ts
│       └── log-store.ts

├── adapters/
│   ├── fake/
│   │   └── FakeAgentAdapter.ts   # 可脚本化控制的测试 Adapter
│   ├── codex/
│   │   └── CodexSdkAdapter.ts    # Codex SDK 子进程 Adapter
│   └── claude/
│       └── ClaudeCliAdapter.ts   # Claude stream-json Adapter

├── workspace/
│   ├── WorktreeManager.ts        # git worktree 生命周期
│   ├── WorkspaceLease.ts         # 租约管理（防并发）
│   └── GitInspector.ts           # git status/diff/log 查询

├── process/
│   ├── ProcessManager.ts         # 子进程管理
│   └── cancellation.ts           # 超时 + 取消传播

├── policy-engine/
│   ├── CommandPolicyEngine.ts    # 命令过滤
│   ├── FileScopeValidator.ts     # 路径验证
│   └── SecretScanner.ts          # 密钥扫描

├── discovery/
│   └── AgentDiscoveryService.ts  # Agent 扫描 + 探测

└── logging/
    └── StructuredLogger.ts       # JSON 格式日志
```

### 3.4 CLI Layer (`src/cli/`)

**依赖 infrastructure**

```
cli/
├── main.ts                       # 入口 + 命令路由
├── commands/
│   ├── run.ts                    # harness run
│   ├── status.ts                 # harness status
│   ├── approve.ts                # harness approve
│   ├── cancel.ts                 # harness cancel
│   └── config.ts                 # harness config
└── output.ts                     # 终端输出格式化
```

### 3.5 Local API (`src/local-api/`)

**依赖 application**

```
local-api/
└── HarnessApi.ts                 # 内部 API facade（供测试 + 未来 TUI/HTTP）
```

### 3.6 Testing Kit (`src/testing-kit/`)

**依赖 domain/contracts**

```
testing-kit/
├── AdapterContractTest.ts        # Adapter 契约测试套件
├── FakeAgentFactory.ts           # FakeAdapter 工厂辅助
└── TestFixtures.ts               # 共享测试数据
```

---

## 4. 依赖方向可视化

```
                        cli/           local-api/
                         │                │
                         ▼                ▼
                    ┌────────────────────────┐
                    │    infrastructure/      │
                    │  (adapters, persistence,│
                    │   workspace, process,   │
                    │   policy-engine,        │
                    │   discovery, logging)   │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │    application/         │
                    │  (commands, services,   │
                    │   orchestration)        │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │    domain/              │
                    │  (contracts, fsm,       │
                    │   policies)             │
                    └────────────────────────┘
                                ▲
                                │
                    ┌────────────────────────┐
                    │    testing-kit/         │
                    └────────────────────────┘
```

---

## 5. 关键模块耦合度分析

| 模块 A | 模块 B | 耦合度 | 说明 |
|--------|--------|--------|------|
| domain/contracts | domain/state-machine | 低 | state-machine 只使用 contracts 中的状态枚举 |
| domain/contracts | domain/policies | 低 | policies 只使用 contracts 中的基础类型 |
| application/commands | domain/contracts | 中 | 每个 command 返回正确的 event 类型 |
| application/commands | infrastructure/persistence | 中 | command handler 调用 event-store 写入 |
| application/services | domain/state-machine | 高（合理） | transition-service 封装所有状态转换 |
| infrastructure/adapters | domain/contracts | 低 | 只实现 AgentAdapter 接口 |
| infrastructure/workspace | infrastructure/process | 低 | 各自独立 |
| cli | infrastructure | 中 | CLI 组合 infrastructure 各模块 |

---

## 6. 为什么选择单 Package 分层

| 考量 | 分析 |
|------|------|
| Foundation Release 模块数量 | ~15 个模块，单 package 足够 |
| 构建复杂性 | 单 package 零构建配置开销 |
| 循环依赖检测 | TypeScript project references 或 eslint import/no-cycle |
| 发布需求 | Foundation 不发布独立 npm 包 |
| 未来扩展 | 当 Adapter 需要独立发布时，拆分 `@harness/adapter-*` |
| 测试速度 | 单 package 共享 tsconfig，无跨 package 编译 |

---

## 7. 禁止的模式

```
❌ domain/ 引用 infrastructure/
❌ domain/ 引用 application/
❌ application/ 引用 infrastructure/
❌ contracts 引用具体 Adapter 实现
❌ 任何模块直接通过字符串 "claude-code"/"codex" 做 if-else 分发
   （必须通过 AgentAdapter 接口 + RuntimeProfile 多态）
```
