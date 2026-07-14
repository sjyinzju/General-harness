# Adapter Contract — Agent Harness

> **文档类型**: 接口规范
> **版本**: v1.0
> **日期**: 2026-07-14
> **稳定性**: Foundation Release 冻结

---

## 1. 契约概述

`AgentAdapter` 是 Harness 与所有 Agent CLI/SDK 之间的统一接口。所有 Agent 实现（Fake、Codex、Claude、未来扩展）都必须实现此接口。

此接口是 Foundation Release 中**最高优先级的冻结契约**。

---

## 2. AgentAdapter 接口

```typescript
interface AgentAdapter {
  // ── 标识 ──────────────────────────────────────────
  /** Adapter 类型标识，如 "codex-sdk", "claude-cli", "fake" */
  readonly kind: string;

  // ── 能力发现 ──────────────────────────────────────
  /** 检测 Agent 是否可执行 */
  detect(binaryPath?: string): Promise<DetectionResult>;

  /** 获取 Agent 版本号 */
  getVersion(): Promise<string>;

  /** 读取 Agent 配置引用（不读密钥值） */
  inspectConfiguration(): Promise<AgentConfigInfo>;

  /** 检查认证状态 */
  checkAuthentication(): Promise<AuthCheckResult>;

  /** 运行微型任务验证能力 */
  probe(tempDir: string): Promise<ProbeResult>;

  // ── 执行 ──────────────────────────────────────────
  /** 创建 Agent 会话 */
  startSession(
    profile: RuntimeProfile,
    opts: SessionOptions
  ): Promise<AgentSession>;

  /** 发送任务信封给 Agent */
  sendTask(
    session: AgentSession,
    envelope: TaskEnvelope
  ): Promise<void>;

  /** 接收 Agent 的流式事件 */
  receiveEvents(session: AgentSession): AsyncIterable<AgentEvent>;

  /** 中断 Agent（graceful stop） */
  interrupt(session: AgentSession): Promise<void>;

  /** 强制取消 Agent */
  cancel(session: AgentSession): Promise<void>;

  // ── 资源管理 ──────────────────────────────────────
  /** 清理会话资源 */
  dispose(session: AgentSession): Promise<void>;
}
```

---

## 3. 支持类型

### 3.1 DetectionResult

```typescript
interface DetectionResult {
  found: boolean;
  binaryPath?: string;
  error?: string;
}
```

### 3.2 AgentConfigInfo

```typescript
interface AgentConfigInfo {
  /** 已配置的 Provider 名称 */
  provider?: string;
  /** API Base URL（如经代理） */
  baseUrl?: string;
  /** 配置的默认 Model ID */
  model?: string;
  /** 认证方式 */
  authMode: "login" | "api_key_env" | "keychain" | "oauth" | "none";
  /** 配置文件路径（引用，不包含内容） */
  configFilePath?: string;
  /** 附加配置 */
  extra: Record<string, unknown>;
}
```

### 3.3 AuthCheckResult

```typescript
interface AuthCheckResult {
  authenticated: boolean;
  method?: string;
  provider?: string;
  error?: string;
}
```

### 3.4 ProbeResult

```typescript
interface ProbeResult {
  status: "passed" | "degraded" | "failed";
  checks: {
    readRepo: boolean;
    createFile: boolean;
    executeTest: boolean;
    outputJsonSchema: boolean;
    interruptAndResume: boolean;
    budgetStop: boolean;
  };
  errorSummary?: string;
  durationMs: number;
}
```

### 3.5 SessionOptions

```typescript
interface SessionOptions {
  /** Agent 的工作目录 */
  workingDirectory: string;
  /** 环境变量覆盖 */
  env?: Record<string, string>;
  /** 超时（毫秒） */
  timeoutMs?: number;
  /** Agent 级别的 max turns */
  maxTurns?: number;
  /** 额外参数（透传给 Agent CLI） */
  extraArgs?: string[];
}
```

### 3.6 AgentSession

```typescript
interface AgentSession {
  /** 唯一会话 ID */
  sessionId: string;
  /** 关联的 Runtime Profile ID */
  profileId: string;
  /** 会话开始时间 */
  startedAt: string;
  /** Agent 原生会话标识（如 Claude Code 的 session_id） */
  nativeSessionId?: string;
  /** 会话是否处于活跃状态 */
  isActive: boolean;
  /** Adapter 私有状态 */
  _internal?: unknown;
}
```

---

## 4. Adapter 实现规范

### 4.1 生命周期

```
detect() → getVersion() → inspectConfiguration() → checkAuthentication()
                                                          │
                                                          ▼
                                                    probe()  ← 在临时仓库
                                                          │
                                                          ▼
                                              startSession()  ← 任务 worktree
                                                          │
                                              ┌───────────────┼───────────────┐
                                              ▼               ▼               ▼
                                          sendTask()    receiveEvents()   interrupt()
                                              │                              │
                                              └──────────────┬───────────────┘
                                                             ▼
                                                         dispose()
```

### 4.2 错误处理

- 所有方法**必须**抛出有意义的错误，而不能静默吞掉异常
- 子进程异常退出 → 抛出 `AgentProcessError`（包含 exit code、stderr 引用）
- 超时 → 抛出 `AgentTimeoutError`
- 不可恢复的错误 → 抛出 `AgentFatalError`（触发 reconciliation）

### 4.3 资源清理

- `dispose()` 必须：
  - 终止子进程（如果存活）
  - 清理临时文件
  - 关闭管道
  - 释放所有资源
- `dispose()` 必须是幂等的（多次调用不抛错）
- `cancel()` 会先尝试 `interrupt()`，超时后强制 `dispose()`

### 4.4 重入与线程安全

- 同一个 `AgentSession` 的 `sendTask` / `receiveEvents` / `interrupt` / `cancel` 不能并发调用
- Harness 确保：同一 session 的调用是串行的
- 不同 session 的调用可以由 Harness 并行执行

---

## 5. FakeAgentAdapter

`FakeAgentAdapter` 是**生产代码**（不是测试代码），用于：

- Adapter 契约的验证测试
- Golden Path 端到端测试（在 CI 中始终运行）
- 当真实 Agent 不可用时的降级路径

```typescript
interface FakeAgentScript {
  /** 预设的 AgentEvent 序列 */
  events: AgentEvent[];
  /** 预设的文件创建 */
  filesToCreate?: { path: string; content: string }[];
  /** 预设的延迟（模拟真实 Agent 执行时间） */
  delayMs?: number;
  /** 模拟失败 */
  shouldFail?: { afterEventIndex: number; error: string };
}

class FakeAgentAdapter implements AgentAdapter {
  kind = "fake";

  /** 设置预设行为，在 startSession 之前调用 */
  setScript(script: FakeAgentScript): void;

  // ... AgentAdapter 方法实现
}
```

---

## 6. CodexSdkAdapter

### 6.1 实现概述

- 使用 Codex SDK 的 subprocess 模式
- 通过 `codex exec --json` 或 SDK 编程接口
- 解析 JSONL/JSON-RPC 输出转为统一 AgentEvent 格式

### 6.2 关键行为

| 场景 | 行为 |
|------|------|
| Codex 未安装 | `detect()` 返回 `{ found: false }` |
| Codex 已安装但未登录 | `checkAuthentication()` 返回 `{ authenticated: false }` |
| Codex session 超时 | 抛出 `AgentTimeoutError`，保留部分输出 |
| Codex 进程异常退出 | 抛出 `AgentProcessError`，保留 stderr |
| 用户取消 | 发送 SIGTERM → 等待 5s → SIGKILL |

---

## 7. ClaudeCliAdapter

### 7.1 实现概述

- 使用 `claude -p --input-format stream-json --output-format stream-json`
- 子进程 stdin 写入 JSON 消息，stdout 解析 stream-json 事件
- 支持 `--resume` 会话续接
- 支持 `--model`、`--effort`、`--permission-mode acceptEdits`

### 7.2 关键行为

| 场景 | 行为 |
|------|------|
| Claude CLI 未安装 | `detect()` 返回 `{ found: false }` |
| Claude CLI 已安装但未登录 | `checkAuthentication()` 返回 `{ authenticated: false }` |
| Stream 解析错误 | 跳过无法解析的行，记录 warning |
| Result 事件 is_error=true | 将 error 作为 AgentEvent(type="error") 发出 |
| 子进程管道断开 | 清理会话，抛出 `AgentProcessError` |

### 7.3 Stream-JSON 协议

发送格式（stdin）：
```json
{"type":"user","message":{"role":"user","content":"任务描述"}}
```

接收格式（stdout，每行一个 JSON 对象）：
```json
{"type":"assistant","message":{"content":[{"type":"text","text":"..."}]}}
{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"...","content":"..."}]}}
{"type":"result","result":"最终文本","is_error":false}
```

---

## 8. 契约测试套件

参见 `docs/testing/adapter-contract-tests.md`。

每个 Adapter 实现**必须**通过以下测试：

1. `detect()` 返回合理结果 (found true/false)
2. `getVersion()` 返回非空字符串
3. `probe()` 在临时仓库中不修改用户文件
4. `startSession()` → `sendTask()` → `receiveEvents()` 完整流程
5. `interrupt()` 在超时内停止 Agent
6. `cancel()` 强制终止 Agent
7. `dispose()` 幂等性
8. 协议错误有合理的错误类型（非泛型 Error）
