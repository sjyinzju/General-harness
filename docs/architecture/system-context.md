# System Context — Agent Harness 系统上下文

> **文档类型**: 架构文档 (C4 Level 1)
> **版本**: v1.0
> **日期**: 2026-07-14

---

## 1. 系统定位

Agent Harness 是一个**本地 Agent 编排运行时**。它作为用户与多个本地 Agent CLI/SDK 之间的中间层，负责接收一次需求描述，将任务拆解、分派、隔离执行、验证和集成。

```
                          ┌─────────────────────┐
                          │    User (Developer)  │
                          │    - 人类开发者       │
                          └──────────┬──────────┘
                                     │
                          harness CLI 命令
                          (run, status, approve, cancel, config)
                                     │
                                     ▼
┌────────────────────────────────────────────────────────────────┐
│                     Agent Harness System                        │
│                     (本地编排运行时)                              │
│                                                                │
│  职责:                                                          │
│  - 发现和管理本地 Agent CLI/SDK                                   │
│  - 项目目标定义和任务 DAG 管理                                    │
│  - Git worktree 隔离和并发控制                                   │
│  - 确定性验收和证据收集                                           │
│  - 崩溃恢复和审计追踪                                             │
└────────────┬──────────────┬──────────────┬──────────────────────┘
             │              │              │
     stream-json      SDK/JSON-RPC     未来扩展
     subprocess       subprocess       (ACP/Gemini/...)
             │              │              │
             ▼              ▼              ▼
┌─────────────────┐ ┌─────────────┐ ┌─────────────────┐
│  Claude Code CLI │ │ Codex CLI   │ │  Future Agents  │
│  (本地安装)      │ │ (本地安装)   │ │  (Gemini, etc.)  │
│  + Anthropic     │ │ + OpenAI    │ │                 │
│  + 自定义 API    │ │ + 自定义 API│ │                 │
└─────────────────┘ └─────────────┘ └─────────────────┘
             │              │              │
             ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────┐
│              Git Repository + Worktrees                      │
│              (被操作的目标代码库)                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. 外部系统与 Actor

| Actor/系统 | 关系 | 交互方式 |
|-----------|------|---------|
| **用户（开发者）** | Harness 的唯一人类用户 | CLI (`harness run/status/approve/cancel/config`) |
| **Claude Code CLI** | 被 Harness 作为子进程调用 | `claude -p --input-format stream-json --output-format stream-json` |
| **Codex CLI** | 被 Harness 作为子进程调用 | Codex SDK subprocess |
| **Git** | Harness 使用 Git 管理 worktree 和版本控制 | `git worktree`, `git branch`, `git add/commit/merge` |
| **文件系统** | 持久化状态、日志、配置 | `.harness/` 目录、`~/.harness/config.json` |
| **SQLite** | 事件存储、状态投影、审计日志 | 嵌入式数据库 |
| **LLM API Provider** | 间接通过 Agent CLI 调用，Harness 不直接访问 | Agent CLI 自行管理认证和调用 |

---

## 3. 核心数据流

```
用户: harness run "需求描述"
  │
  ▼
CLI → Application Facade (HarnessApi)
  │
  ├─→ [Command] CreateProject
  │     └─→ EventStore: ProjectCreated
  │
  ├─→ GoalContractManager: 创建 Goal Contract v1
  │
  ├─→ AgentDiscoveryService: 获取 AVAILABLE Runtime Profiles
  │
  ├─→ [PlanningProvider]: 生成 Task DAG (Foundation: FakePlanningProvider)
  │
  ├─→ [等待用户审批]
  │
  ├─→ Scheduler: 遍历 DAG，找到 READY 任务
  │     │
  │     ├─→ WorktreeManager: 创建 Git worktree
  │     ├─→ WorkspaceLease: 获取租约
  │     ├─→ ProcessManager: 启动 Agent 子进程
  │     │     └─→ AgentAdapter.sendTask(TaskEnvelope)
  │     │           └─→ Stream<AgentEvent>
  │     │
  │     ├─→ Agent 返回 → TaskResult (声明)
  │     │
  │     ├─→ PolicyEngine: 验证文件范围
  │     ├─→ VerificationService: 运行 acceptanceChecks
  │     │     └─→ VerificationEvidence
  │     │
  │     ├─→ TransitionService: SUBMITTED → VERIFIED → COMMITTED
  │     ├─→ WorktreeManager: git add + commit (Harness 独占)
  │     │
  │     └─→ DAG 下一个任务
  │
  └─→ Integrator: merge → final verification → DONE
```

---

## 4. 系统边界与约束

### Harness 的职责

- 发现和探测 Agent 能力
- 管理项目、Goal Contract、Task DAG 生命周期
- 创建和清理 Git worktree
- 控制 Agent 子进程的启动、监控和终止
- 执行确定性验收检查
- 执行所有 Git 操作（add/commit/merge）
- 执行安全策略（路径验证、命令过滤、密钥扫描）
- 持久化所有状态事件以支持恢复和审计

### Harness 不负责的

- 直接调用 LLM API（由 Agent CLI 自行管理）
- 存储 API Key（只存储 provider/model 引用）
- 理解自然语言需求（Foundation Release 阶段）
- 提供操作系统级沙箱（容器/VM）
- 多用户管理

---

## 5. 部署视图

```
开发机 (Windows / macOS / Linux)
│
├── ~/.harness/
│   ├── config.json             # 用户配置
│   └── db/
│       └── harness.db          # SQLite 数据库
│
├── /usr/local/bin/harness      # CLI 可执行文件 (npm install -g)
│
└── 项目目录/
    ├── .harness/               # 项目级 Harness 数据
    │   ├── worktrees/          # Git worktree 目录
    │   ├── artifacts/          # 共享产物
    │   ├── logs/               # Agent transcript 日志
    │   └── checkpoints/        # 完整状态快照
    ├── .git/                   # Git 仓库
    └── src/...                 # 项目源码
```
