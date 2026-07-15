# Security Controls — Agent Harness

> **版本**: v2.0
> **日期**: 2026-07-15
> **修订**: 将"七层安全边界"修正为"分层策略控制"；明确 Foundation Release 不提供 OS 级沙箱

---

## 重要声明

**Foundation Release 不提供操作系统级安全沙箱（容器、VM、cgroup、seccomp、namespace）。**

Agent 子进程运行在与 Harness 相同的操作系统用户下。以下控制机制是**策略控制和检测检查**，不是 OS 级安全隔离。

真正的安全沙箱属于 **Production Release**。

---

## 分层控制

### Layer 1: Agent 工具权限 (Preventive)

- Agent 的 `allowedTools` 白名单
- 限制 Agent 可调用的工具类型
- **局限**：Agent 如果拥有 shell 工具，可以通过 shell 执行任意命令绕过此层

### Layer 2: 子进程与工作区隔离 (Preventive)

- 每个写入任务创建独立 Git worktree
- 子进程 `cwd` 锁定为 worktree 目录
- 环境变量最小化（不传递 API Key 等敏感变量）
- **局限**：Agent 仍在同一 OS 用户下运行，可访问用户的其他文件（受 OS 权限限制）

### Layer 3: 命令策略 (Preventive Checks)

- 高风险命令模式拦截（`rm -rf /`、`git push --force`、`curl | sh` 等）
- 所有命令记录到 audit_log（参数摘要、exit code、stdout/stderr 引用、时间）
- **局限**：命令过滤是模式匹配，可能被绕过；不是系统调用级别的拦截

### Layer 4: 文件路径检查 (Preventive Checks)

- 所有文件路径规范化 + 前缀检查
- 禁止访问 `.git/`、`.harness/`、`~/.ssh/`、`~/.aws/` 等
- **局限**：检查发生在 Harness 层面，Agent 在文件系统上有 OS 级访问权限

### Layer 5: Git Diff 检查 (Detective)

- 任务前后 `git diff`，验证变更在 `allowedPaths` 内
- 检测意外的大范围修改
- **局限**：Agent 可以修改文件然后还原，diff 检查无法检测"修改-还原"的痕迹

### Layer 6: 密钥扫描 (Detective)

- Commit 前扫描 diff 中的密钥/Token/Password 模式
- **局限**：模式匹配无法覆盖所有密钥格式；零日 API Key 格式可能漏过

### Layer 7: 验证失败隔离 (Post-Execution)

- VERIFIED 不通过 → worktree 不合并
- FAILED_TERMINAL → worktree 标记为废弃
- 合并失败 → 回滚 integration 分支
- **局限**：失败的代码已经存在于 worktree 中（虽然未合并）

---

## 完整沙箱抽象（Production Release）

Foundation Release 定义但不实现 Sandbox 抽象接口：

```rust
/// Sandbox abstraction — not enforced in Foundation Release.
/// In Production Release, this will be backed by containers, VMs,
/// or OS-level isolation (cgroups, seccomp, namespaces).
trait Sandbox {
    /// Create an isolated execution environment
    fn create(&self, spec: SandboxSpec) -> Result<Box<dyn SandboxInstance>>;
}

struct SandboxSpec {
    /// Allowed filesystem paths (read-only)
    readable_paths: Vec<PathBuf>,
    /// Allowed filesystem paths (read-write)
    writable_paths: Vec<PathBuf>,
    /// Network access policy
    network: NetworkPolicy,
    /// Resource limits
    limits: ResourceLimits,
    /// Allowed syscalls (seccomp filter, Linux only)
    allowed_syscalls: Option<Vec<String>>,
}

/// Foundation Release implementation: passes through to host OS.
/// Documents clearly that it provides NO isolation.
struct HostSandbox;
impl Sandbox for HostSandbox {
    fn create(&self, _spec: SandboxSpec) -> Result<Box<dyn SandboxInstance>> {
        // NOTICE: No sandboxing in Foundation Release.
        // Agent process runs with same OS permissions as Harness.
        Ok(Box::new(HostSandboxInstance))
    }
}
```

---

## Foundation Release 安全能力声明

### ✅ 已实现

- Agent 工具白名单限制
- 独立 Git worktree 隔离
- 环境变量最小化
- 危险命令模式拦截（模式匹配级别）
- 命令执行审计记录（参数摘要、exit code、stdout/stderr 引用、时间）
- 文件路径逃逸检测（路径规范化 + prefix 检查）
- Git diff 审查（允许路径验证）
- 密钥模式扫描（常见模式匹配）
- 验证失败后 worktree 不合并
- Harness 独占 git commit
- 不读取/存储 Agent API Key

### ❌ 未实现（文档中如实声明）

- 操作系统级沙箱（容器/VM/cgroup/namespace/seccomp）
- 网络访问控制（防火墙规则）
- 系统调用过滤
- 资源配额硬限制（CPU/内存/磁盘）
- 只读文件系统挂载
- 非 root/独立用户运行

### ⚠️ 重要警告

```
Foundation Release 不能防御完全恶意的本地 Agent 进程。
路径、命令和 diff 检查是 policy controls，不是 OS 级隔离。
恶意 Agent 理论上可以：
  - 通过 shell 执行任意命令（受限于命令模式过滤的可绕过性）
  - 读取用户的其他文件（受限于 OS 权限）
  - 消耗系统资源（CPU/内存/磁盘）
真正安全沙箱属于 Production Release。
```
