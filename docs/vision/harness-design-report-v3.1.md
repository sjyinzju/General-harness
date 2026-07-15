# Agent Harness 系统 —— Foundation Release 架构规划

> **版本**: v3.1 — Foundation Baseline
> **日期**: 2026-07-14
> **取代**: v3.0 (`harness-design-report.md`)
> **定位**: 确定性的本地 Agent 编排运行时 —— Foundation Release 建立整个系统的领域边界、核心接口、状态机、事件协议、持久化方式、Agent Adapter、工作区管理、调度、安全、验证和恢复基础设施。

---

## 修订摘要（v3.0 → v3.1 核心变更）

| # | 变更领域 | v3.0 | v3.1 | 原因 |
|---|---------|------|------|------|
| 1 | 版本命名 | "MVP" | "Foundation Release" | 当前目标不是最小可用产品，而是完整的架构基线 |
| 2 | Agent 建模 | "Claude Code + DeepSeek"作为推荐组合 | Runtime Profile 统一建模，不预设组合 | 组合必须经过实际探测才能进入调度池 |
| 3 | Adapter 策略 | Claude SDK/stream-json 并列 | CodexSdkAdapter 优先 + ClaudeCliAdapter + ClaudeSdkAdapter 独立 | 区分 SDK 认证与 CLI 认证 |
| 4 | Worktree 命名 | `TASK-A1-project-scaffold-claude-sonnet/` | `TASK-014-auth-callback/` | Worktree 绑定任务，Agent/Model 信息存数据库 |
| 5 | Git 所有权 | 子 Agent 执行 git commit | Harness 独占 git commit/merge/rebase | Worker 不可修改 Git 历史 |
| 6 | Goal Contract | 声明"不可变"但允许原地更新 | 每个版本不可变，通过 Change Request 创建新版 | 消除矛盾 |
| 7 | 状态机 | 项目 12 状态 + 任务 11 状态 | 项目 21 状态 + 任务 17 状态 + 完整转换表 | 覆盖暂停/恢复/崩溃/孤儿等真实场景 |
| 8 | 安全模型 | Hooks 作为主要安全边界 | 7 层纵深防御 | Hooks 只是一层，不可单独依赖 |
| 9 | 持久化 | "SQLite + append-only event log" | 完整事件溯源：command/domain event/projection/audit event 四类区分 | 支撑崩溃恢复和审计 |
| 10 | UI 依赖 | TUI 仪表板作为阶段目标 | 核心系统不依赖 TUI/Daemon/Web UI | 核心必须可在普通 CLI 和进程内测试 |
| 11 | 实施计划 | 按功能拆分 5 个 Phase | 按依赖关系拆分为 F0-F10 共 11 个阶段 | 基础设施先行 |

---

## 目录

1. [版本层次定义](#一版本层次定义)
2. [愿景与设计哲学](#二愿景与设计哲学)
3. [Foundation Release 范围](#三foundation-release-范围)
4. [Runtime Profile 模型](#四runtime-profile-模型)
5. [Adapter 架构](#五adapter-架构)
6. [Git 所有权与 Worktree 模型](#六git-所有权与-worktree-模型)
7. [Goal Contract 版本化](#七goal-contract-版本化)
8. [状态机完整设计](#八状态机完整设计)
9. [安全纵深防御](#九安全纵深防御)
10. [事件模型与持久化](#十事件模型与持久化)
11. [模块边界与依赖规则](#十一模块边界与依赖规则)
12. [核心契约摘要](#十二核心契约摘要)
13. [实施计划（F0-F10）](#十三实施计划f0-f10)
14. [Golden Path](#十四golden-path)
15. [核心设计原则](#十五核心设计原则)
16. [决策冻结与推迟清单](#十六决策冻结与推迟清单)
17. [过度设计风险](#十七过度设计风险)
18. [Foundation 完成后的真实能力与明确限制](#十八foundation-完成后的真实能力与明确限制)

---

## 一、版本层次定义

本项目的发布分为三个层次，当前只实现 Foundation Release：

### Foundation Release（当前）
**目标**：建立完整领域框架、核心基础设施、稳定契约和可验证 Golden Path。

- 完整的状态机、事件存储、持久化
- Agent Adapter 契约 + FakeAdapter + CodexSdkAdapter + ClaudeCliAdapter
- Git worktree 隔离、进程管理、安全策略引擎
- DAG 基础模型和确定性调度
- 确定性验证、Harness 控制的 commit
- 崩溃恢复和检查点
- CLI 基础命令
- Golden Path 端到端可运行

### Functional Release（后续）
**目标**：多 Agent DAG、真实规划、审查修复和基础路由可以日常使用。

- LLM 驱动的 Planner/Reviewer/Debugger
- 历史评分路由
- 自动修复循环
- TUI 仪表板
- 用户审批工作流
- Goal Contract 澄清对话

### Production Release（远期）
**目标**：完整安全沙箱、动态路由、可观测性、TUI/Web UI、长期运行和跨平台完善。

- 操作系统级沙箱
- OpenTelemetry 全链路追踪
- Web UI / Tauri 桌面界面
- Temporal 持久工作流
- 多机分布式执行
- 团队协作
- 插件市场

---

## 二、愿景与设计哲学

### 核心原则

```
          确定性软件系统 (80%+)              LLM 管理能力 (<20%)
     ┌─────────────────────────┐    ┌──────────────────────────┐
     │ • 状态机转换规则          │    │ • 需求理解与澄清           │
     │ • DAG 依赖计算            │    │ • 架构设计与任务拆解        │
     │ • 子进程生命周期管理       │    │ • 代码实现                 │
     │ • Git worktree 隔离       │    │ • 代码审查（语义理解部分）   │
     │ • 权限与预算硬限制         │    │ • 调试与根因分析           │
     │ • 确定性验收               │    │ • 仅在歧义时做模糊决策      │
     │ • 所有 Git 操作            │    │ • 重规划建议               │
     │ • 事件存储与审计追踪        │    │                           │
     └─────────────────────────┘    └──────────────────────────┘
```

**LLM 可以建议，但状态变更由确定性规则执行。Worker 返回的 TaskResult 是声明，不是事实。**

---

## 三、Foundation Release 范围

### 必须真实实现（32 项）

1. CLI 基础命令（`harness run/status/approve/cancel/config`）
2. 配置加载和校验（YAML/JSON Schema）
3. SQLite migration 框架
4. Append-only event store
5. State projection（从事件重建当前状态）
6. 项目和任务双层状态机（含完整转换表）
7. Command handler（命令→事件→状态投影）
8. AgentAdapter 契约接口
9. FakeAgentAdapter（可脚本化控制的行为）
10. CodexSdkAdapter（真实子进程集成）
11. ClaudeCliAdapter（stream-json 集成）
12. AgentDiscoveryService（扫描 + 探测 + Runtime Profile）
13. Runtime Profile probe（微型任务验证能力）
14. Git repository inspection（读取仓库状态）
15. WorktreeManager（创建/列出/锁定/清理）
16. WorkspaceLease（租约机制防并发冲突）
17. ProcessManager（子进程启停、超时、强制终止、进程树清理）
18. Cancellation 和 timeout 传播
19. DAG 基础模型（Task 节点 + Dependency 边）
20. 确定性 Scheduler（DAG 拓扑排序 + 硬过滤路由）
21. Policy engine（budget/command/file-scope 策略）
22. Command execution record（参数摘要、exit code、stdout/stderr 引用、时间）
23. File scope validation（路径规范化和逃逸检测）
24. Diff inspection（任务执行前后的 git diff）
25. 确定性 verification（acceptanceChecks 执行 + exit code 判断）
26. Harness-owned commit（Harness 独占 git add + commit + 保存 hash）
27. Checkpoint（完整状态快照保存和恢复）
28. Crash reconciliation（启动时识别 ORPHANED/LEASED/RUNNING 状态并恢复）
29. Structured logging（JSON 格式，按 run_id 分区）
30. Adapter contract test kit（可复用的适配器测试套件）
31. CLI integration tests
32. 一个端到端 Golden Path（FakeAdapter）

### 高级功能使用简单确定性实现

- **Planner**：手工创建的 Task DAG（`FakePlanningProvider`）
- **Routing**：基于规则的硬过滤（不包含历史评分学习）
- **Reviewer**：仅确定性验证（不包含 LLM Judge）
- **Repair**：状态和接口已定义，但不实现自动连续修复循环
- **Budget**：执行次数 + 时间 + 显式 limit，不强求精确美元统计
- **Local API**：仅实现内部 application facade，不实现 HTTP/gRPC

### 明确不在此版本的内容

- LLM 驱动的自动规划
- 自动修复循环
- TUI/Web UI/Daemon 模式
- Agent 历史评分与学习路由
- 完整的 LLM Judge reviewer
- ACP/Gemini/Aider Adapter
- MCP Server 暴露
- 操作系统级沙箱
- OpenTelemetry 集成
- Temporal 工作流引擎
- 多机分布式执行
- PTY Adapter

---

## 四、Runtime Profile 模型

### 建模原则

**不预设** "Claude Code + DeepSeek" 或 "Codex + GPT-4" 是合法组合。

每个 Runtime Profile 必须经过实际探测（Probe）确认以下全部维度：

```typescript
interface RuntimeProfile {
  id: string;                    // 全局唯一标识
  agentKind: string;             // "claude-code" | "codex" | "gemini" | "acp" | "custom"
  agentVersion: string;          // 从 --version 获取
  adapterKind: string;           // "claude-cli" | "codex-sdk" | "claude-sdk" | "acp" | "pty"
  provider: string;              // "anthropic" | "openai" | "deepseek" | "zhipu" | "custom"
  model: string;                 // 实际配置的 model ID
  baseUrl?: string;              // API 端点（如经代理）
  authMode: "login" | "api_key_env" | "keychain" | "oauth" | "none";
  authState: "unauthenticated" | "authenticated" | "expired" | "unknown";

  // 能力集
  capabilities: {
    workspaceModes: ("read" | "write" | "shell")[];
    structuredOutput: boolean;   // 是否支持 JSON Schema 输出
    streaming: boolean;          // 是否支持流式输出
    sessionResume: boolean;      // 是否支持会话续接
    maxConcurrency: number;      // 同一 profile 最大并发执行数
    sandboxMode: "none" | "workspace-write" | "container";
    supportedLanguages: string[];// 探测确认的语言支持
    mcpTools: string[];          // 已连接的 MCP 工具列表
  };

  // 探测结果
  probe: {
    status: "untested" | "passed" | "degraded" | "failed";
    testedAt?: string;
    checks: {
      readRepo: boolean;         // 可读取仓库结构
      createFile: boolean;       // 可在临时目录创建文件
      executeTest: boolean;      // 可执行测试命令
      outputJsonSchema: boolean; // 可输出符合 JSON Schema 的响应
      interruptAndResume: boolean;// 可中断并恢复
      budgetStop: boolean;       // 可在达到预算后停止
    };
    errorSummary?: string;
  };

  // 调度状态
  status: "DETECTED" | "CONFIGURED" | "AUTHENTICATED" | "PROBED" | "AVAILABLE" | "DEGRADED" | "UNAVAILABLE";
  degradedReasons?: string[];

  // 运行历史（调度优化用，Foundation 阶段收集但不用于自动决策）
  history?: {
    totalTasks: number;
    successRate: number;
    avgDurationMs: number;
    lastUsedAt?: string;
  };

  createdAt: string;
  updatedAt: string;
}
```

### 发现流程

1. **扫描可执行程序**：PATH、npm global、pip/uv、Cargo、Homebrew/Scoop、自定义路径
2. **detect()**：调用 `--version`，确认可执行
3. **inspectConfiguration()**：读取配置文件引用（provider、base URL、model ID、auth mode），**不读密钥值**
4. **checkAuthentication()**：调用 Agent 自身的认证状态接口
5. **probe()**：在临时仓库运行微型任务，验证全部能力维度
6. **状态分级**：DETECTED → CONFIGURED → AUTHENTICATED → PROBED → AVAILABLE/DEGRADED
7. **定时刷新**：AVAILABLE profile 每 N 小时重新探测；调度失败时即时重新探测

### 自定义 Provider 的处理

用户通过代理网关使用 DeepSeek/GLM 等 custom provider 时，Profile 中的 provider 字段为实际提供商名称，model 字段为用户配置的模型 ID。这些组合**只有经过完整 Probe 后**才能标记为 AVAILABLE。

---

## 五、Adapter 架构

### Adapter 类型与优先级

```
Foundation Release 实现的 Adapter：

┌─────────────────────────────────────────────────────────────┐
│                    AgentAdapter (contract)                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  FakeAgentAdapter     CodexSdkAdapter    ClaudeCliAdapter   │
│  (测试用，完全可控)    (真实子进程)       (stream-json)       │
│                                                             │
│  优先级: P0             优先级: P1         优先级: P2         │
│  用途: 契约测试、       用途: Codex SDK    用途: Claude CLI   │
│        Golden Path      通过子进程调用     通过 stream-json  │
│                         实现               协议调用           │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│  定义边界但不在 Foundation 实现：                              │
│  - ClaudeSdkAdapter   (需解决 SDK 认证 ≠ CLI 登录的问题)       │
│  - CodexCliAdapter (`codex exec --json`, 当前版本)            │
│  - CodexAppServerAdapter (Deep Integration via app-server, 后续版本) │
│  - AcpAdapter          (标准化 ACP 协议，后续版本)             │
│  - GeminiAdapter       (后续版本)                             │
│  - PtyAdapter          (最后兼容手段，后续版本)                │
└─────────────────────────────────────────────────────────────┘
```

### Adapter 契约核心方法

```typescript
interface AgentAdapter {
  readonly kind: string;

  // 生命周期
  detect(): Promise<DetectionResult>;
  getVersion(): Promise<string>;
  inspectConfiguration(): Promise<AgentConfigInfo>;
  checkAuthentication(): Promise<AuthCheckResult>;
  probe(tempDir: string): Promise<ProbeResult>;

  // 执行
  startSession(profile: RuntimeProfile, opts: SessionOptions): Promise<AgentSession>;
  sendTask(session: AgentSession, envelope: TaskEnvelope): Promise<void>;
  receiveEvents(session: AgentSession): AsyncIterable<AgentEvent>;
  interrupt(session: AgentSession): Promise<void>;
  cancel(session: AgentSession): Promise<void>;

  // 资源管理
  dispose(session: AgentSession): Promise<void>;
}
```

### CodexSdkAdapter 要点

- 通过子进程调用 Codex SDK
- 不假设 Codex 登录可以复用为第三方 SDK 认证
- session/thread 管理由 Harness 控制
- 所有工具调用事件通过 AgentEvent 流式返回

### ClaudeCliAdapter 要点

- 使用 `claude -p --input-format stream-json --output-format stream-json`
- 子进程 stdin/stdout 通信
- 解析 stream-json 事件转为统一 AgentEvent 格式
- 支持 `--resume` 会话续接
- 支持 `--model` 和 `--effort` 参数
- 支持 `--permission-mode acceptEdits` 用于自动化场景

---

## 六、Git 所有权与 Worktree 模型

### 边界划分

| 操作 | 谁负责 | 说明 |
|------|--------|------|
| 创建分支 | Harness | 基于 integration 分支创建 |
| 创建 worktree | Harness | git worktree add |
| 修改文件 | Worker Agent | 在其 worktree 内 |
| 运行允许的命令 | Worker Agent | 受 command policy 约束 |
| git status/diff | Harness | 检查做了什么修改 |
| 越界修改检查 | Harness | 验证修改的文件在 allowedPaths 内 |
| 执行验收检查 | Harness | acceptanceChecks |
| git add | Harness | 选择性添加允许路径内的文件 |
| git commit | **Harness 独占** | Worker 不可做 |
| git push | **严禁** | 任何 Agent 都不可 |
| git reset --hard | **严禁** | 任何 Agent 都不可 |
| git rebase | Harness | 仅在架构变更时 |
| 合并/cherry-pick | Harness | 所有多分支操作 |
| 清理 worktree | Harness | 任务完成后 |

### Worktree 命名

**仅绑定 Task 或 Workstream，不绑定 Agent 或 Model：**

```
.harness/worktrees/
├── TASK-014-auth-callback/
├── TASK-018-dashboard-shell/
├── TASK-021-integration-tests/
└── WORKSTREAM-authentication/    # 多任务串行共享
```

Agent 分派历史存储在 SQLite 数据库中的 `task_assignments` 表。

### 安全规则

- Harness **不得**自动提交用户主工作区原有的 dirty changes
- Worker Agent **默认不得**执行 git 命令（commit/push/reset/rebase/checkout 其他分支）
- 如果任务确实需要提交（如 monorepo 内部工具），必须显式声明在 scope 中
- 所有路径必须规范化并验证不能逃逸允许目录（`../` 攻击防护）

---

## 七、Goal Contract 版本化

### 版本模型

- 每个 Goal Contract Version **不可变**
- 新需求通过 **Change Request** 创建新版本
- 每个版本记录：`supersedes`、`changeReason`、`affectedTasks`、`planRevision`
- 任何 Task 必须记录自己执行时引用的 `goalContractVersion` 和 `planVersion`

```typescript
interface GoalContractVersion {
  version: number;
  objective: string;
  deliverables: string[];
  acceptance: string[];
  constraints: string[];
  nonGoals: string[];
  techStack?: Record<string, string>;
  createdAt: string;
  createdBy: "user" | "orchestrator";
}

interface ChangeRequest {
  id: string;
  reason: string;
  requestedBy: "user" | "orchestrator";
  changes: {
    field: string;           // 被修改的字段路径
    before: unknown;
    after: unknown;
  }[];
  affectedTasks: string[];   // 受影响的 Task ID
  newGoalContractVersion: number;
  planRevision: number;
  status: "proposed" | "approved" | "rejected";
  createdAt: string;
}
```

---

## 八、状态机完整设计

### 项目级状态（21 个）

```
CREATED → CLARIFYING → GOAL_LOCKED → PLANNING
                                          ↓
                                     AWAITING_APPROVAL
                                          ↓
                                      SCHEDULING
                                          ↓
                                       RUNNING
                                          ↓
                          ┌───────────────┼───────────────┐
                          ▼               ▼               ▼
                       PAUSING        INTEGRATING      BLOCKED
                          │               │
                          ▼               ▼
                       PAUSED          VERIFYING
                          │          ┌────┼────┐
                          ▼          ▼    ▼    ▼
                      RESUMING     DONE  REPAIRING  FAILED
                          │               │
                          ▼               ▼
                       RUNNING         RUNNING
                          │               │
                      DEGRADED            │
                          │               │
                      RUNNING             │
                          │               │
                      RECOVERING ←────────┘ (崩溃后)
                          │
                          ▼
                       RUNNING

终端状态: DONE | CANCELLED | FAILED
中断状态: PAUSING → PAUSED → RESUMING → RUNNING
异常状态: DEGRADED | BLOCKED | RECOVERING → RUNNING
取消状态: CANCELLING → CANCELLED
```

### 任务级状态（17 个）

```
PENDING → READY → LEASED → RUNNING → AWAITING_INPUT → RUNNING
                                              │
                                              ▼
                                          SUBMITTED
                                              │
                                              ▼
                                          VERIFYING
                                     ┌────────┼────────┐
                                     ▼        ▼        ▼
                                  VERIFIED  FAILED_RETRYABLE  FAILED_TERMINAL
                                     │        │               │
                                     ▼        ▼               ▼
                                  COMMITTED  RUNNING         BLOCKED
                                     │
                                     ▼
                                  MERGING → MERGED

特殊状态: ORPHANED (进程崩溃,无 owner) | SUPERSEDED (被取代) | CANCELLED
```

### 状态转换规则

**核心原则**：业务代码不能直接修改状态字段，只能通过 `TransitionService` 进行。

```typescript
interface TransitionService {
  // 每个 transition 返回 StateTransitionResult（成功）或 TransitionError（非法转换）
  transitionProject(projectId: string, to: ProjectStatus, context: TransitionContext): Promise<StateTransitionResult>;
  transitionTask(taskId: string, to: TaskStatus, context: TransitionContext): Promise<StateTransitionResult>;
}

interface TransitionContext {
  actor: "harness" | "orchestrator" | "system";
  reason: string;
  evidence?: VerificationEvidence[];
  idempotencyKey: string;
}
```

**关键转换的前置条件（示例）**：

| 转换 | 前置条件 |
|------|---------|
| RUNNING → SUBMITTED | Agent session 已结束；有 stdout/stderr 日志；有 changedFiles 列表 |
| SUBMITTED → VERIFIED | 所有 acceptanceChecks exitCode===0；diff 在 allowedPaths 内；密钥扫描通过；Reviewer（如有）给出 PASS |
| VERIFIED → COMMITTED | Harness 成功执行 git add + commit；commit hash 已保存 |
| COMMITTED → MERGED | 无冲突合并到 integration 分支；集成测试通过 |
| LEASED → ORPHANED | 进程已退出但未正常 finish；通过 crash reconciliation 检测到 |

### 崩溃恢复（Reconciliation）

Harness 启动时检查所有非终端状态的任务：

```
1. LEASED / RUNNING：检查子进程是否仍存活
   ├─ 存活 → 维持状态（可能被另一个 Harness 实例管理）
   └─ 不存活 → 标记为 ORPHANED → 通知 Orchestrator 决策（重试/放弃/请求用户）

2. ORPHANED：
   ├─ retryCount < maxRetries → 重置为 READY → 可以重新调度
   └─ retryCount >= maxRetries → 标记为 FAILED_TERMINAL → 通知用户

3. VERIFYING / COMMITTING / MERGING：
   ├─ 检查是否有中间产物（commit hash, merge 状态）
   ├─ 可恢复 → 从上次成功的步骤继续
   └─ 不可恢复 → 回退到 SUBMITTED → 重新验证
```

### 幂等要求

- 所有 `transition*()` 调用通过 `idempotencyKey` 保证幂等
- 重复调用同一转换（相同 key、相同 from→to）返回已有结果，不重复执行
- 所有外部命令（git、test runner）执行前检查是否已有结果

---

## 九、安全纵深防御

Hooks 只是安全的一层。完整的安全模型为 7 层：

```
Layer 1: Agent 工具权限与 Hook
  ├─ Agent 的工具白名单（allowedTools）
  ├─ before_tool_call / after_tool_call hooks
  └─ 但 Agent 自身可能绕过（如直接使用 shell 执行任意命令）

Layer 2: 子进程/工作目录/环境变量隔离
  ├─ 每个任务独立 worktree
  ├─ 环境变量最小化（不传递 API Key 等敏感变量）
  └─ cwd 锁定为 worktree 目录

Layer 3: 命令策略 (CommandPolicy)
  ├─ 白名单模式或黑名单模式
  ├─ 高风险命令拦截（rm -rf, git push --force, curl | bash, eval）
  └─ 命令参数摘要记录

Layer 4: 文件系统路径检查 (FileScopePolicy)
  ├─ 所有读写路径规范化
  ├─ 验证不逃逸 allowedPaths
  ├─ 禁止访问 .git 目录内部
  └─ 禁止访问 .harness/ 目录

Layer 5: 完成后 Git diff 检查
  ├─ 计算任务前后的完整 diff
  ├─ 验证所有变更文件在 allowedPaths 内
  ├─ 检测意外的大范围修改
  └─ 检测二进制文件变更

Layer 6: 密钥扫描
  ├─ 在 commit 前扫描 diff 中的密钥模式
  ├─ 检测 API Key / Token / Password 明文
  └─ 发现则阻止 commit 并标记 FAILED

Layer 7: 验证失败后的回滚或废弃 worktree
  ├─ VERIFIED 不通过则 worktree 不合并
  ├─ FAILED_TERMINAL 则清理 worktree
  └─ 合并失败则回滚 integration 分支
```

Foundation Release **可以暂不提供完整操作系统级沙箱**（容器/VM），但所有能力和限制必须如实描述。

---

## 十、事件模型与持久化

### 事件类型区分

| 类型 | 用途 | 示例 | 存储位置 |
|------|------|------|---------|
| **Command** | 用户/系统的意图 | `CreateProject`, `DispatchTask`, `ApprovePlan` | event_log 表 |
| **Domain Event** | 系统中已发生的事实 | `ProjectCreated`, `TaskDispatched`, `TaskVerified` | event_log 表 |
| **Projection** | 从事件重建的当前状态 | `project_current_state`, `task_current_state` | projections 表（可重建） |
| **Audit Event** | 安全审计轨迹 | `FileAccessed`, `CommandExecuted`, `SecretScanResult` | audit_log 表 |
| **Agent Stream Event** | Agent 运行时的实时事件 | `tool_use`, `assistant_message`, `error` | agent_events 表 + 文件系统 |

### 持久化架构

```
┌─────────────────────────────────────────────┐
│              SQLite Database                 │
├─────────────────────────────────────────────┤
│  event_log         (append-only, 不可变)     │
│  ├─ id, stream_id, event_type, payload      │
│  ├─ version (乐观锁), timestamp              │
│  └─ idempotency_key (唯一约束)               │
│                                              │
│  projections       (current state, 可重建)   │
│  ├─ projects, tasks, workspaces              │
│  ├─ runtime_profiles                         │
│  └─ goal_contract_versions, change_requests   │
│                                              │
│  audit_log         (安全审计, 不可变)         │
│  ├─ command_executions                       │
│  ├─ file_accesses                            │
│  └─ secret_scans                             │
│                                              │
│  agent_events      (Agent 运行时事件)         │
│  └─ tool_use, assistant_message, error       │
├─────────────────────────────────────────────┤
│  文件系统                                     │
│  .harness/                                    │
│  ├── worktrees/     (Git worktree 目录)       │
│  ├── artifacts/     (任务产出物引用)           │
│  ├── logs/          (Agent transcript 全文)    │
│  └── checkpoints/   (完整状态快照)            │
└─────────────────────────────────────────────┘
```

### 写入模型

- **单写入者**：Harness 主进程独占 SQLite 写入
- **Agent 子进程绝不访问 SQLite**
- **事务**：每个 command 处理在一个 SQLite 事务中
- **幂等**：通过 `idempotency_key` 保证命令幂等
- **乐观版本**：stream 用 `version` 字段做乐观锁
- **Schema migration**：版本化 migration 脚本

---

## 十一、模块边界与依赖规则

### 推荐项目结构：单 package 分层 + 少量独立 package

**选择理由**：Foundation Release 的模块数量可控（~15 个），不需要 monorepo 的构建复杂性。单 package 内通过文件夹分层保持依赖方向，只有真正需要独立发布或完全无耦合的模块才拆分为独立 package。

### 源码分层

```
src/
├── domain/                    # 领域模型（零外部依赖）
│   ├── contracts/             # 核心接口与类型
│   │   ├── AgentAdapter.ts
│   │   ├── AgentIdentity.ts
│   │   ├── CapabilitySet.ts
│   │   ├── RuntimeProfile.ts
│   │   ├── TaskEnvelope.ts
│   │   ├── TaskResult.ts
│   │   ├── GoalContract.ts
│   │   ├── Project.ts
│   │   ├── Task.ts
│   │   ├── Workspace.ts
│   │   ├── DomainEvent.ts
│   │   └── ...
│   ├── state-machine/         # 状态机（纯函数）
│   │   ├── project-fsm.ts
│   │   ├── task-fsm.ts
│   │   └── transition-rules.ts
│   └── policies/              # 策略类型定义
│       ├── BudgetPolicy.ts
│       ├── PermissionPolicy.ts
│       ├── CommandPolicy.ts
│       └── FileScopePolicy.ts
│
├── application/               # 应用层（依赖 domain）
│   ├── commands/              # Command handlers
│   │   ├── create-project.ts
│   │   ├── dispatch-task.ts
│   │   ├── verify-task.ts
│   │   └── ...
│   ├── services/              # 应用服务
│   │   ├── transition-service.ts
│   │   ├── scheduler.ts
│   │   ├── verification.ts
│   │   └── reconciliation.ts
│   └── orchestration/         # 编排逻辑（后续版本引入 LLM）
│       └── fake-planning-provider.ts
│
├── infrastructure/            # 基础设施（依赖 application + domain）
│   ├── persistence/
│   │   ├── sqlite/
│   │   │   ├── event-store.ts
│   │   │   ├── projections.ts
│   │   │   ├── migrations/
│   │   │   └── connection.ts
│   │   └── file/
│   │       ├── checkpoint-store.ts
│   │       └── artifact-store.ts
│   ├── adapters/              # Agent Adapter 实现
│   │   ├── fake/
│   │   │   └── FakeAgentAdapter.ts
│   │   ├── codex/
│   │   │   └── CodexSdkAdapter.ts
│   │   └── claude/
│   │       └── ClaudeCliAdapter.ts
│   ├── workspace/
│   │   ├── WorktreeManager.ts
│   │   ├── WorkspaceLease.ts
│   │   └── GitInspector.ts
│   ├── process/
│   │   ├── ProcessManager.ts
│   │   └── cancellation.ts
│   ├── policy-engine/
│   │   ├── CommandPolicyEngine.ts
│   │   ├── FileScopeValidator.ts
│   │   └── SecretScanner.ts
│   ├── discovery/
│   │   └── AgentDiscoveryService.ts
│   └── logging/
│       └── StructuredLogger.ts
│
├── cli/                       # CLI 入口（依赖 infrastructure）
│   ├── commands/
│   │   ├── run.ts
│   │   ├── status.ts
│   │   ├── approve.ts
│   │   ├── cancel.ts
│   │   └── config.ts
│   └── main.ts
│
├── local-api/                 # 内部 API facade（依赖 application）
│   └── HarnessApi.ts          # 供测试和未来 TUI/HTTP 使用
│
└── testing-kit/               # 测试工具包
    ├── AdapterContractTest.ts
    ├── FakeAgentFactory.ts
    └── TestFixtures.ts
```

### 依赖方向

```
domain/contracts  ←  application  ←  infrastructure/adapters  ←  cli
                                                              ←  local-api
        ↑                                                        ↑
        └────────── testing-kit ─────────────────────────────────┘
```

**核心 domain 不依赖具体的 Claude、Codex、SQLite、Git CLI 或 UI。**
**所有箭头单向，严格禁止循环依赖。**

### 需要独立 package 的模块

仅在以下情况拆分为独立 package：
- `testing-kit/`：需要被外部 adapter 测试引用时
- 未来单独的 Adapter 实现（如 `@harness/adapter-gemini`）

---

## 十二、核心契约摘要

（完整契约定义参见 `docs/architecture/adapter-contract.md` 和各契约自己的文档。）

| 契约 | 层 | 生产者 | 消费者 | 持久化 | 版本化 |
|------|---|--------|--------|--------|--------|
| RuntimeProfile | domain | AgentDiscoveryService | Scheduler, CLI | ✅ | ✅ schema version |
| ProbeResult | domain | AgentAdapter.probe() | AgentDiscoveryService | ✅ | ❌ |
| AgentAdapter | domain | 各 Adapter 实现 | ProcessManager | ❌ | ✅ 接口版本 |
| AgentSession | domain | AgentAdapter.startSession() | ProcessManager | ❌ | ❌ |
| AgentEvent | domain | AgentAdapter.receiveEvents() | ProcessManager, CLI | ✅ (agent_events) | ✅ schema version |
| TaskEnvelope | domain | Scheduler | AgentAdapter.sendTask() | ❌ | ✅ schema version |
| TaskResult | domain | Worker Agent (声明) | VerificationService | ✅ (audit) | ✅ schema version |
| AcceptanceCheck | domain | Planner (人工/Fake) | VerificationService | ✅ | ❌ |
| VerificationEvidence | domain | VerificationService | TransitionService | ✅ | ❌ |
| GoalContractVersion | domain | 用户/Orchestrator | Planner | ✅ | ✅ version |
| ChangeRequest | domain | 用户/Orchestrator | TransitionService | ✅ | ❌ |
| DomainEvent | domain | Command Handlers | EventStore, Projections | ✅ | ✅ schema version |
| EventEnvelope | domain | EventStore | Projections, Audit | ✅ | ❌ |
| StateTransitionResult | domain | TransitionService | Command Handlers | ✅ | ❌ |
| WorkspaceLease | domain | WorktreeManager | Scheduler | ✅ | ❌ |
| Project | domain | Command Handlers | Projection | ✅ | ❌ |
| Task | domain | Command Handlers | Projection | ✅ | ❌ |

**关键注意**：Worker 返回的 `TaskResult` 是**声明**，不是事实。所有声明必须由 Harness 通过 `VerificationEvidence` 独立验证。

---

## 十三、实施计划（F0-F10）

### 阶段划分原则

按**依赖关系**拆分，不按 UI 功能拆分。每个阶段的输出物必须可独立验证。

```
F0: Repository, Toolchain, Quality Gates
    输出: 项目仓库、TypeScript 配置、lint/formatter、CI 骨架、空 SQLite migration 框架
    依赖: 无

F1: Domain Contracts & Dependency Rules
    输出: 所有 domain/contracts 类型定义、依赖规则文档、循环依赖检查脚本
    依赖: F0

F2: Event Store, Persistence, State Machine
    输出: SQLite event store、projection 引擎、完整状态机（21+17 状态）、migration v1
    依赖: F1

F3: Process, Policy, Workspace, Git
    输出: ProcessManager、PolicyEngine、WorktreeManager、GitInspector、WorkspaceLease
    依赖: F1 (不依赖 F2——这些模块操作文件系统，不操作事件存储)

F4: Agent Runtime & Fake Adapter
    输出: AgentAdapter 契约、FakeAgentAdapter、AgentSession 生命周期、contract test kit
    依赖: F1

F5: Codex & Claude Adapters
    输出: CodexSdkAdapter、ClaudeCliAdapter、两者通过 contract test kit 验证
    依赖: F4

F6: Discovery, Runtime Profile, Probe
    输出: AgentDiscoveryService、RuntimeProfile probe 流程、配置加载
    依赖: F5

F7: Scheduler, DAG, Verification, Commit
    输出: DAG 拓扑排序、确定性 Scheduler、VerificationService、Harness 独占 commit
    依赖: F2 + F3 + F6

F8: CLI & Application Facade
    输出: 所有 CLI 命令、HarnessApi、配置校验
    依赖: F7

F9: Golden Path, Recovery, Contract Tests
    输出: 端到端 Golden Path 测试、crash reconciliation 实现、contract test 完整套件
    依赖: F8

F10: Documentation, Risk Review, Foundation Acceptance
    输出: 所有规划文档终稿、风险审查报告、Foundation 验收通过
    依赖: F9
```

---

## 十四、Golden Path

### Golden Path 完整流程

```
1.  用户: harness run "创建一个简单的 Node.js HTTP 服务器"
2.  CLI: 创建 Project → CREATE_PROJECT command → ProjectCreated event
3.  CLI: 创建 Goal Contract v1
4.  CLI: 加载配置 → 加载 Runtime Profiles → 选择第一个 AVAILABLE 的（FakeAdapter）
5.  Orchestrator (FakePlanningProvider): 生成简单 Task DAG:
      TASK-001: 初始化项目 + 创建 server.js
      TASK-002: 添加测试
6.  CLI: 保存 Plan v1
7.  Scheduler: TASK-001 → READY → 分配 Runtime Profile → LEASED
8.  WorktreeManager: 创建 worktree .harness/worktrees/TASK-001-init-server/
9.  ProcessManager: 调用 AgentAdapter.startSession() → AgentSession
10. ProcessManager: 调用 AgentAdapter.sendTask(TaskEnvelope)
11. ProcessManager: 接收流式 AgentEvent → FakeAdapter 返回模拟文件创建
12. Agent session 结束 → TaskStatus: SUBMITTED
13. WorkspaceManager: git diff → 收集 changedFiles
14. PolicyEngine: FileScopeValidator → 验证路径在 allowedPaths 内
15. VerificationService: 运行 acceptanceChecks → exitCode 0 → VERIFIED
16. WorkspaceManager: git add + git commit (Harness 独占) → COMMITTED
17. Scheduler: TASK-002 → 同样流程
18. WorkspaceManager: merge TASK-001 + TASK-002 → integration 分支
19. 最终 verification → Project: DONE
20. 输出结果摘要给用户

重启恢复 Golden Path:
21. 模拟进程崩溃 (kill)
22. harness resume → 从 SQLite 恢复状态 → reconciliation 发现 ORPHANED 任务
23. 重新调度 ORPHANED 任务 → 正常完成
```

### Golden Path 必须覆盖的测试场景

| 路径 | Adapter | 环境要求 |
|------|---------|---------|
| FakeAdapter Golden Path | FakeAgentAdapter | 无（CI 始终运行） |
| CodexSdkAdapter Integration | CodexSdkAdapter | Codex CLI 已安装且已认证 |
| ClaudeCliAdapter Integration | ClaudeCliAdapter | Claude CLI 已安装且已登录 |
| Agent UNAVAILABLE | FakeAgentAdapter (模拟) | 无 |
| Crash Recovery | FakeAgentAdapter | 无 |

---

## 十五、核心设计原则

### 实现纪律

```
禁止:
❌ 未使用的抽象（没有调用方和测试的接口/类）
❌ 空壳 package（只有 index.ts 没有实现）
❌ 只有接口而没有调用方和测试
❌ any 绕过类型检查
❌ 静默吞掉子进程异常
❌ Agent 直接写 SQLite
❌ Agent 控制 Harness 状态（Agent 返回声明，Harness 验证后更新状态）
❌ Worker 创建 git commit
❌ 读取和保存 API Key 明文
❌ UI 驱动核心流程
❌ 自然语言字符串作为状态值

必须:
✅ 所有跨边界数据经 Schema 校验（Zod）
✅ 所有外部命令记录参数摘要、exit code、stdout/stderr 引用、开始/结束时间
✅ 所有路径规范化并验证不逃逸允许目录
✅ 所有资源有 owner、lease 和清理策略
✅ 所有重试有 idempotency key 和最大次数
✅ 所有状态恢复能识别 ORPHANED execution
✅ 所有后续高级功能通过核心接口扩展，不绕过核心
```

---

## 十六、决策冻结与推迟清单

### 必须在编码前冻结的决策

| # | 决策 | 理由 |
|---|------|------|
| AD-001 | TypeScript 为开发语言 | 与 Claude/Codex SDK 同生态；单线程事件循环天然适合编排 |
| AD-002 | SQLite 为持久化引擎 | 零配置、嵌入式、单写入者模型天然适合 |
| AD-003 | Zod 为 Schema 校验库 | TypeScript 原生类型推断，运行时校验 |
| AD-004 | 单 package 分层结构 | Foundation Release 规模可控 |
| AD-005 | AgentAdapter 接口契约（见 adapter-contract.md） | 所有 Adapter 的稳定性依赖于此 |
| AD-006 | 状态机转换必须通过 TransitionService | 安全性和幂等的前提 |
| AD-007 | Git 操作归属 Harness（Worker 不可 commit） | 安全模型的基石 |
| AD-008 | Event Store 使用 append-only + idempotency key | 审计和恢复的基础 |
| AD-009 | Worktree 绑定 Task/Workstream，不绑定 Agent | 任务可被不同 Agent 重试 |

### 可以推迟的决策

| # | 决策 | 推迟到 | 理由 |
|---|------|--------|------|
| PD-001 | 是否引入 LangGraph | Functional Release | Foundation 的自研状态机足够 |
| PD-002 | 是否引入 Temporal | Production Release | 需要先验证单机可靠性 |
| PD-003 | TUI 框架选择（Ink/Blessed/其他） | Functional Release | 核心不依赖 UI |
| PD-004 | MCP Server 暴露 | Functional Release | 需要先有稳定的内部 API |
| PD-005 | 容器沙箱方案（Docker/Podman） | Production Release | 当前不要求 OS 级隔离 |
| PD-006 | OpenTelemetry 集成 | Functional Release | 结构化日志先满足 Foundation 需求 |
| PD-007 | 多语言支持（Python SDK/CLI 绑定） | Functional Release | TypeScript 优先 |
| PD-008 | 团队协作/多用户模型 | Production Release | 单用户优先 |

---

## 十七、过度设计风险

以下领域存在过度设计风险，需持续关注：

| # | 领域 | 风险 | 缓解 |
|---|------|------|------|
| 1 | Policy Engine | 设计过于通用的规则引擎 | 先用硬编码规则，接口保持简单 |
| 2 | Event Store | 做成完整 CQRS/Event Sourcing 框架 | 只实现 append + projection，不做 event bus |
| 3 | Scheduler | 试图在第一版实现智能并行调度 | 先做简单的拓扑排序 + 串行执行 |
| 4 | Agent Event 类型 | 枚举过多种 Agent 事件 | 从最小集开始（5-8 种），按需添加 |
| 5 | Checkpoint | 实现过于细粒度的增量检查点 | 先做粗粒度完整快照（任务边界） |
| 6 | Plugin 系统 | 过早抽象 Adapter 加载机制 | Foundation 硬编码 Adapter 注册 |
| 7 | Worktree 粒度策略 | 试图自动决策并行 vs 串行 | Foundation 默认一任务一 worktree |
| 8 | Budget 追踪 | 试图精确到美元 | Foundation 只做 maxTurns + maxTime |

---

## 十八、Foundation 完成后的真实能力与明确限制

### 真实能力

```
✅ 发现本地已安装的 Agent CLI（Claude Code、Codex）并探测其能力
✅ 创建项目、定义 Goal Contract、手动创建 Task DAG
✅ 为每个 Task 创建独立的 Git worktree
✅ 调用 Agent 在隔离 worktree 中执行代码任务
✅ 实时接收 Agent 的工作流事件
✅ 执行确定性验收检查（运行测试、lint 等命令）
✅ 由 Harness 执行 git add + commit（Agent 不触碰 git）
✅ 任务级别的崩溃恢复（进程重启后识别并从断点继续）
✅ 路径逃逸防护、命令白名单、密钥扫描
✅ 完整的审计日志（谁在何时做了什么）
✅ 通过 FakeAdapter 在 CI 中始终可测试
```

### 明确不具备的能力

```
❌ 自动理解用户自然语言需求并生成 Task DAG（需要 LLM Planner，Functional Release）
❌ LLM 驱动的代码审查（需要 LLM Reviewer，Functional Release）
❌ 自动修复循环（需要 Repair Loop，Functional Release）
❌ 基于历史数据的智能 Agent 路由（Functional Release）
❌ 实时 TUI 仪表板（Functional Release）
❌ 操作系统级容器沙箱（Production Release）
❌ 多机分布式执行（Production Release）
❌ 支持除 Claude Code 和 Codex 之外的 Agent（后续版本）
❌ 中英文翻译管线（后续版本）
```

---

## 附录：修订历史

| 版本 | 日期 | 变更摘要 |
|------|------|---------|
| v1.0 | 2026-07-14 | 初始构想 |
| v2.0 | 2026-07-14 | 融合第二轮评审，加入 Goal Contract、80/20 原则 |
| v3.0 | 2026-07-14 | 加入澄清/审批阶段、实时仪表板、Worktree 隔离详细设计 |
| v3.1 | 2026-07-14 | 重新定义为 Foundation Release；修正 Agent 建模、Adapter 策略、Git 所有权、Goal Contract 版本化、状态机扩展、安全纵深防御、事件模型、模块边界；新增决策冻结/推迟清单和过度设计风险评估 |
