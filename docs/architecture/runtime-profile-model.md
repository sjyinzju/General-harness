# Runtime Profile Model — Agent Harness

> **文档类型**: 架构规范
> **版本**: v1.0
> **日期**: 2026-07-14

---

## 1. 概述

Runtime Profile 是 Agent Harness 中 Agent 能力的唯一真实来源。每个 Profile 必须经过探测（Probe）确认能力后才能进入调度池。

---

## 2. 完整 Schema

```typescript
// ── Runtime Profile ──────────────────────────────────
interface RuntimeProfile {
  /** 全局唯一 ID，格式: {agentKind}-{adapterKind}-{provider}-{model}-{hash4} */
  id: string;

  /** Agent CLI/SDK 类型 */
  agentKind: "claude-code" | "codex" | "gemini" | "acp" | "custom";

  /** Agent 版本号（从 --version 获取） */
  agentVersion: string;

  /** Adapter 类型 */
  adapterKind: "claude-cli" | "codex-sdk" | "claude-sdk" | "acp" | "pty" | "fake";

  /** 可执行文件路径 */
  binaryPath: string;

  // ── LLM Provider ──────────────────────────────────
  /** Provider 名称（来自 Agent 配置） */
  provider: string;

  /** API Base URL（如有代理/自定义端点） */
  baseUrl?: string;

  /** 当前配置的 Model ID */
  model: string;

  // ── 认证 ──────────────────────────────────────────
  /** 认证方式 */
  authMode: "login" | "api_key_env" | "keychain" | "oauth" | "none";

  /** 认证状态（来自 checkAuthentication） */
  authState: "unauthenticated" | "authenticated" | "expired" | "unknown";

  /** 认证最近验证时间 */
  authCheckedAt?: string;

  // ── 能力集 ────────────────────────────────────────
  capabilities: CapabilitySet;

  // ── 探测结果 ──────────────────────────────────────
  probe: ProbeResult;

  // ── 调度状态 ──────────────────────────────────────
  status: RuntimeProfileStatus;

  /** 降级原因（status 为 DEGRADED 时） */
  degradedReasons?: string[];

  /** 并发限制 */
  concurrency: {
    maxParallel: number;
    currentActive: number;
  };

  // ── 历史数据（调度优化用） ──────────────────────
  history?: {
    totalTasks: number;
    completedTasks: number;
    failedTasks: number;
    successRate: number;          // 0.0 - 1.0
    avgDurationMs: number;
    lastUsedAt?: string;
  };

  // ── 元数据 ────────────────────────────────────────
  createdAt: string;
  updatedAt: string;
  lastProbedAt?: string;
}

// ── 能力集 ──────────────────────────────────────────
interface CapabilitySet {
  /** 工作区访问模式 */
  workspaceModes: ("read" | "write" | "shell")[];

  /** 是否支持结构化输出 (JSON Schema) */
  structuredOutput: boolean;

  /** 是否支持流式事件输出 */
  streaming: boolean;

  /** 是否支持会话续接 (resume) */
  sessionResume: boolean;

  /** 沙箱模式 */
  sandboxMode: "none" | "workspace-write" | "container";

  /** 探测确认的支持语言 */
  supportedLanguages: string[];

  /** 已连接的 MCP 工具名称列表 */
  mcpTools: string[];

  /** 支持的操作系统 */
  supportedPlatforms: ("win32" | "darwin" | "linux")[];

  /** 是否支持审批流暂停/恢复 */
  approvalFlow: boolean;

  /** 是否支持中断 */
  interruptible: boolean;

  /** 最大上下文窗口估计（token 数） */
  estimatedContextWindow?: number;
}

// ── 探测结果 ────────────────────────────────────────
interface ProbeResult {
  status: "untested" | "passed" | "degraded" | "failed";
  testedAt?: string;
  version?: string;
  checks: ProbeChecks;
  errorSummary?: string;
  logs?: string[];
}

interface ProbeChecks {
  /** 能否读取仓库结构 */
  readRepo: boolean;

  /** 能否在临时目录创建文件 */
  createFile: boolean;

  /** 能否执行测试命令 */
  executeTest: boolean;

  /** 能否输出符合指定 JSON Schema 的响应 */
  outputJsonSchema: boolean;

  /** 能否被中断并恢复 */
  interruptAndResume: boolean;

  /** 是否在达到预算限制后停止 */
  budgetStop: boolean;

  /** 能否处理结构化 TaskEnvelope */
  acceptsTaskEnvelope: boolean;
}

// ── 调度状态 ────────────────────────────────────────
type RuntimeProfileStatus =
  | "DETECTED"       // 可执行程序已找到
  | "CONFIGURED"     // 配置已读取
  | "AUTHENTICATED"  // 认证已验证
  | "PROBED"         // 能力探测已完成
  | "AVAILABLE"      // 可进入调度池
  | "DEGRADED"       // 部分能力不可用
  | "UNAVAILABLE";   // 不可用
```

---

## 3. 发现与探测流程

### 3.1 扫描可执行程序

```typescript
class AgentDiscoveryService {
  async scan(): Promise<DiscoveredAgent[]> {
    const discovered: DiscoveredAgent[] = [];

    // 1. 扫描 PATH
    for (const dir of process.env.PATH.split(path.delimiter)) {
      for (const agentName of KNOWN_AGENT_BINARIES) {
        const fullPath = path.join(dir, agentName);
        if (await this.isExecutable(fullPath)) {
          discovered.push({ agentKind: identifyAgentKind(agentName), binaryPath: fullPath });
        }
      }
    }

    // 2. 扫描常见安装目录
    const extraPaths = [
      path.join(os.homedir(), '.local', 'bin'),
      path.join(os.homedir(), 'AppData', 'Local'),
      path.join(os.homedir(), '.cargo', 'bin'),
      // ...平台特定的路径
    ];

    // 3. 用户自定义路径（来自 config.json）
    const customPaths = this.config.agentDiscovery.additionalPaths;

    return discovered;
  }
}
```

### 3.2 探测流程

```typescript
async function probeAgent(adapter: AgentAdapter, binaryPath: string): Promise<ProbeResult> {
  const tempDir = await fs.mkdtemp('.harness-probe-');

  try {
    const checks: ProbeChecks = {
      readRepo: false, createFile: false, executeTest: false,
      outputJsonSchema: false, interruptAndResume: false,
      budgetStop: false, acceptsTaskEnvelope: false
    };

    // 1. 读取仓库结构
    try {
      await adapter.sendTask(session, { goal: 'List the directory structure' });
      checks.readRepo = true;
    } catch { /* leave false */ }

    // 2. 创建文件
    try {
      await adapter.sendTask(session, { goal: 'Create a file named probe.txt with content "ok"' });
      const exists = await fs.pathExists(path.join(tempDir, 'probe.txt'));
      checks.createFile = exists;
    } catch { /* leave false */ }

    // 3. 执行测试
    // ...etc for each check

    return {
      status: computeStatus(checks),
      testedAt: new Date().toISOString(),
      checks
    };
  } finally {
    await fs.rm(tempDir, { recursive: true, force: true });
  }
}
```

---

## 4. 状态转换

```
扫描到可执行程序 → DETECTED
        │
        ▼ (读取配置成功)
    CONFIGURED
        │
        ▼ (认证验证成功)
    AUTHENTICATED
        │
        ▼ (探测全部通过)
      PROBED
        │
        ▼ (手动确认或自动)
    AVAILABLE ←──────────────┐
        │                    │
        ├── (部分能力丢失) → DEGRADED
        │                    │
        └── (完全不可用) → UNAVAILABLE
```

---

## 5. 调度过滤

参见 `docs/architecture/dependency-rules.md`（调度部分）。

Scheduler 通过以下条件筛选可用 Profile：

```typescript
function filterAvailableProfiles(
  profiles: RuntimeProfile[],
  task: Task
): RuntimeProfile[] {
  return profiles
    .filter(p => p.status === "AVAILABLE")
    .filter(p => p.concurrency.currentActive < p.concurrency.maxParallel)
    .filter(p => {
      // 硬过滤条件
      if (task.requiresWrite && !p.capabilities.workspaceModes.includes("write")) return false;
      if (task.requiresShell && !p.capabilities.workspaceModes.includes("shell")) return false;
      if (task.requiresStructuredOutput && !p.capabilities.structuredOutput) return false;
      return true;
    });
}
```

---

## 6. 持久化

Runtime Profile 存储在 SQLite 的 `runtime_profiles` 表中：

```sql
CREATE TABLE runtime_profiles (
  id TEXT PRIMARY KEY,
  agent_kind TEXT NOT NULL,
  agent_version TEXT NOT NULL,
  adapter_kind TEXT NOT NULL,
  binary_path TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  auth_mode TEXT NOT NULL,
  auth_state TEXT NOT NULL,
  capabilities TEXT NOT NULL,       -- JSON
  probe_status TEXT NOT NULL,
  probe_result TEXT,                -- JSON
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

---

## 7. 为什么不能预设 Agent + LLM 组合

**错误假设**：
- "Claude Code + DeepSeek V4 Pro 可以做架构设计"
- "Codex + GPT-4 适合写代码"

**现实**：
- Claude Code 可能通过代理配置使用 DeepSeek，也可能直接使用 Anthropic
- Codex 的 Provider 取决于用户配置（OpenAI / Azure / 自定义）
- 用户可能配置了自定义 base URL 指向本地模型
- 某个组合今天可用，明天可能因为认证过期而不可用

**正确做法**：
- 每个 Profile 必须经过完整的 Probe 流程
- 只有在临时仓库中成功完成微型任务的 Profile 才能标记为 AVAILABLE
- "Claude Code + DeepSeek" 只是 Profile 的 `agentKind + provider + model` 字段值，不是官方的"默认支持组合"
- Harness 对 Agent × Provider × Model 不做任何预设判断
