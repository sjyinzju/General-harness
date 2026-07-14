# Security Boundaries — Agent Harness

> **文档类型**: 架构规范
> **版本**: v1.0
> **日期**: 2026-07-14

---

## 1. 安全模型概述

Agent Harness 管理不受信任的代码生成 Agent。安全模型采用纵深防御策略，**Hooks 是其中一层，不能单独依赖**。

Foundation Release 不提供操作系统级沙箱（容器/VM），但所有安全边界清晰定义，如实描述能力与限制。

---

## 2. 七层纵深防御

```
┌──────────────────────────────────────────────────────────────┐
│ Layer 7: 回滚与废弃                                           │
│ 验证失败 → worktree 不合并 → 隔离区废弃                        │
├──────────────────────────────────────────────────────────────┤
│ Layer 6: 密钥扫描                                            │
│ 在 git commit 前扫描 diff 中的密钥/Token/Password 明文          │
├──────────────────────────────────────────────────────────────┤
│ Layer 5: Git Diff 检查                                       │
│ 任务完成后计算完整 diff → 验证变更在 allowedPaths 内            │
├──────────────────────────────────────────────────────────────┤
│ Layer 4: 文件系统路径检查                                      │
│ 所有路径规范化 → 验证不逃逸 worktree → 禁止访问 .git/.harness/  │
├──────────────────────────────────────────────────────────────┤
│ Layer 3: 命令策略                                             │
│ 高风险命令拦截 (rm -rf, git push -f, curl | bash, eval)       │
│ 所有命令记录参数、exit code、stdout/stderr                     │
├──────────────────────────────────────────────────────────────┤
│ Layer 2: 子进程隔离                                           │
│ 独立 worktree、最小化环境变量、cwd 锁定、无 API Key 传递        │
├──────────────────────────────────────────────────────────────┤
│ Layer 1: Agent 工具权限                                       │
│ allowedTools 白名单、before/after hooks、maxTurns 限制         │
└──────────────────────────────────────────────────────────────┘
```

---

## 3. Layer 1: Agent 工具权限

### 3.1 工具白名单

```typescript
interface ToolPermissionConfig {
  /** 允许的工具名列表 */
  allowedTools: string[];
  /** 是否允许 Agent 使用未在白名单中的工具 */
  allowUnknownTools: boolean;  // Foundation: 始终 false
  /** 最大工具调用次数（防无限循环） */
  maxToolCalls?: number;
}
```

### 3.2 限制

- 工具白名单是 Agent 级别的限制，但 Agent 可以通过 shell 工具执行任意命令绕过
- **因此 Layer 1 不能单独作为安全边界**

---

## 4. Layer 2: 子进程隔离

### 4.1 Worktree 隔离

- 每个写入任务创建独立 Git worktree
- Agent 子进程的 `cwd` 锁定为该 worktree 目录
- Agent 无法访问其他 worktree（通过路径和进程隔离）

### 4.2 环境变量最小化

```typescript
// Agent 子进程只继承安全的环境变量
const SAFE_ENV_KEYS = [
  "PATH", "HOME", "USER", "TMP", "TEMP",
  "LANG", "TERM", "SHELL", "NODE_ENV"
  // 明确排除：ANTHROPIC_API_KEY, OPENAI_API_KEY, 等
];

function buildAgentEnv(baseEnv: NodeJS.ProcessEnv): Record<string, string> {
  const env: Record<string, string> = {};
  for (const key of SAFE_ENV_KEYS) {
    if (baseEnv[key]) env[key] = baseEnv[key]!;
  }
  return env;
}
```

### 4.3 限制

- 子进程仍在同一操作系统用户下运行
- 可以读取用户的其他文件（如果 OS 权限允许）
- Foundation Release 不提供 OS 级沙箱

---

## 5. Layer 3: 命令策略

### 5.1 命令拦截规则

```typescript
// 始终拦截的命令模式（危险操作）
const BLOCKED_COMMAND_PATTERNS = [
  /rm\s+-rf\s+\//,           // 删除根目录
  /git\s+push\s+--force/,    // 强制推送
  /git\s+reset\s+--hard/,    // 硬重置
  /curl.*\|\s*(ba)?sh/,      // curl pipe shell
  /wget.*\|\s*(ba)?sh/,      // wget pipe shell
  /eval\s+/,                 // eval
  /chmod\s+777/,             // 宽松权限
  />\s*\/dev\/sda/,          // 写裸设备
  /mkfs\./,                  // 格式化
  /dd\s+if=/,                // 裸设备操作
  /:\(\)\s*\{/,              // fork bomb
];

// 需要审批的命令模式
const DANGEROUS_COMMAND_PATTERNS = [
  /rm\s+-rf/,                // 递归删除
  /sudo\s+/,                 // 提权
  /npm\s+publish/,           // 发布
  /docker\s+(rm|prune)/,     // 删除容器/镜像
  /kubectl\s+delete/,        // 删除 K8s 资源
  /git\s+push/,              // 推送
  /shutdown|reboot|halt/,    // 系统命令
];
```

### 5.2 命令审计

所有 Agent 发出的命令（通过 bash tool）必须记录：

```typescript
interface CommandExecutionRecord {
  id: string;
  taskId: string;
  command: string;           // 完整命令
  sanitizedCommand: string;  // 脱敏后的命令（用于日志）
  workingDirectory: string;
  startTime: string;
  endTime: string;
  exitCode: number;
  stdoutRef: string;         // 文件系统路径
  stderrRef: string;
  durationMs: number;
  timeoutMs: number;
  wasBlocked: boolean;       // 是否被策略拦截
  wasApproved: boolean;      // 是否经过用户审批
}
```

---

## 6. Layer 4: 文件系统路径检查

### 6.1 路径验证

```typescript
class FileScopeValidator {
  validate(allowedPaths: string[], worktreeRoot: string): PathValidator {
    return {
      /** 验证文件操作是否在允许范围内 */
      check(filePath: string, operation: "read" | "write"): PathValidationResult {
        // 1. 规范化路径
        const normalized = path.resolve(worktreeRoot, filePath);

        // 2. 防止目录逃逸
        if (!normalized.startsWith(path.resolve(worktreeRoot))) {
          return { allowed: false, reason: "PATH_ESCAPE", path: normalized };
        }

        // 3. 禁止访问 .git 目录
        if (normalized.includes(".git/") || normalized.endsWith(".git")) {
          return { allowed: false, reason: "GIT_DIR_FORBIDDEN", path: normalized };
        }

        // 4. 禁止访问 .harness 目录
        if (normalized.includes(".harness/")) {
          return { allowed: false, reason: "HARNESS_DIR_FORBIDDEN", path: normalized };
        }

        // 5. 检查是否匹配 allowedPaths glob
        const relativePath = path.relative(worktreeRoot, normalized);
        if (!matchesGlob(relativePath, allowedPaths)) {
          return { allowed: false, reason: "NOT_IN_ALLOWED_PATHS", path: normalized };
        }

        return { allowed: true };
      }
    };
  }
}
```

### 6.2 禁止区域

| 路径模式 | 原因 |
|---------|------|
| `.git/` | 防止 Agent 篡改 Git 历史 |
| `.harness/` | 防止 Agent 读取其他任务的数据 |
| `~/.harness/` | 防止 Agent 访问全局 Harness 配置 |
| `~/.ssh/` | 防止 Agent 窃取 SSH 密钥 |
| `~/.aws/` | 防止 Agent 窃取 AWS 凭证 |
| 环境变量中的密钥路径 | 动态检测 |

---

## 7. Layer 5: Git Diff 检查

### 7.1 任务前后 Diff

```typescript
class DiffInspector {
  /** 获取任务执行前后的完整 diff */
  async getTaskDiff(taskId: string): Promise<{
    files: ChangedFile[];
    summary: { additions: number; deletions: number; filesChanged: number };
  }>;

  /** 验证所有变更在允许路径内 */
  async validateFileScope(task: Task, diff: TaskDiff): Promise<ScopeValidationResult>;
}
```

### 7.2 异常检测

- 大量文件修改（>50 个文件）→ 标记为可疑
- 二进制文件新增 → 审查
- `.env` / `credentials` 文件修改 → 阻止

---

## 8. Layer 6: 密钥扫描

### 8.1 扫描时机

- 在 `git commit` 之前（`VERIFIED → COMMITTING` 转换前）
- 扫描 `git diff --cached` 的内容

### 8.2 检测模式

```typescript
const SECRET_PATTERNS = [
  /[A-Za-z0-9_]{20,}={0,2}/,                         // 疑似 base64 token
  /sk-[A-Za-z0-9]{32,}/,                              // OpenAI/Anthropic API Key
  /ghp_[A-Za-z0-9]{36}/,                              // GitHub Personal Access Token
  /-----BEGIN (RSA|DSA|EC|OPENSSH) PRIVATE KEY-----/, // SSH 私钥
  /password\s*[:=]\s*["'][^"']+["']/i,                // 硬编码密码
  /secret\s*[:=]\s*["'][^"']+["']/i,
  /token\s*[:=]\s*["'][^"']+["']/i,
  /AKIA[0-9A-Z]{16}/,                                 // AWS Access Key
];
```

### 8.3 发现密钥时的处理

1. 创建 `AuditEvent(type=SecretScanResult, found=true)`
2. 阻止 VERIFIED → COMMITTED 转换
3. 任务进入 FAILED_RETRYABLE
4. 通知 Orchestrator（含发现位置但不含密钥内容）

---

## 9. Layer 7: 回滚与废弃

- VERIFIED 不通过 → worktree 不合并 → 在重试耗尽后清理
- FAILED_TERMINAL → 丢弃 worktree，分支保留用于审计
- 合并失败 → 回滚 integration 分支到合并前状态
- 所有回滚操作记录在 audit_log

---

## 10. 认证安全

- Harness **不读取、不存储、不传输**任何 Agent 的 API Key
- 配置文件只存储 provider/model 引用（名称字符串）
- Agent 自身的认证由 Agent CLI 管理（keychain/env/lockfile）
- Harness 的 Orchestrator LLM 配置（API Key）存储在 `~/.harness/config.json`，权限 600

---

## 11. Foundation Release 的安全能力声明

### 已实现

```
✅ Agent 工具白名单限制
✅ 独立 Git worktree 隔离
✅ 环境变量最小化
✅ 危险命令模式拦截
✅ 命令执行审计记录（参数摘要、exit code、stdout/stderr 引用、时间）
✅ 文件路径逃逸检测
✅ Git diff 审查（允许路径验证）
✅ 密钥模式扫描
✅ 验证失败后的 worktree 不合并
✅ Harness 独占 git commit
✅ 不读取/存储 Agent API Key
```

### 未实现（后续版本）

```
❌ 操作系统级沙箱（容器/VM/cgroup）
❌ 网络访问控制（防火墙规则）
❌ 系统调用过滤（seccomp）
❌ 资源配额（CPU/内存/磁盘硬限制）
❌ 只读文件系统挂载
❌ 非 root/独立用户运行
```
