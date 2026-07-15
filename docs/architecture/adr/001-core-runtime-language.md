# ADR-001: Core Runtime Language — Rust

> **状态**: Accepted
> **日期**: 2026-07-15
> **取代**: 旧 ADR-001 (TypeScript as Primary Language)
> **决策者**: Harness 架构设计 (Architecture Correction Review)

---

## Context

Agent Harness 是一个本地 Agent 编排运行时。其核心特征：
- 作为**独立 CLI 工具**发布，不依赖 Node.js/Python 运行时
- 管理**多个 Agent 子进程**，需要精确的进程树控制
- **长时间运行**（数小时到数天），需要稳定的内存管理
- **跨平台**（Windows + macOS + Linux），需要单二进制分发
- 需要**确定性状态机**和并发正确性

## Decision Drivers (按权重排序)

| # | 驱动因素 | 权重 |
|---|---------|------|
| 1 | 长时间运行的本地进程稳定性 | 🔴 最高 |
| 2 | 多子进程并发、取消和进程树管理 | 🔴 最高 |
| 3 | JSONL / JSON-RPC 流式协议处理 | 🔴 最高 |
| 4 | Git worktree 与文件系统操作 | 🟠 高 |
| 5 | Windows、macOS、Linux 跨平台 | 🟠 高 |
| 6 | 单二进制安装和升级 | 🟠 高 |
| 7 | 交互式 CLI/TUI | 🟡 中 |
| 8 | 状态机和并发正确性 | 🟡 中 |
| 9 | SQLite 嵌入式数据库 | 🟡 中 |
| 10 | Agent SDK 直接接入便利性 | 🟢 低(1) |
| 11 | 开发速度 | 🟢 低(2) |

> (1) Agent SDK 接入通过 **CLI stream-json / JSON-RPC 子进程协议**实现，不依赖 SDK 的原生语言绑定。只在某个 SDK 具有不可替代的能力时才引入 sidecar。
> (2) 开发速度让步于运行时稳定性——这是长期运行的本地基础设施工具。

---

## Comparison Matrix

| 维度 | Rust | Go | TypeScript (Node.js) |
|------|------|----|-----------------------|
| **长期进程稳定性** | 🟢 无 GC、无内存泄漏、所有权系统 | 🟡 GC 暂停低但存在、goroutine 泄漏风险 | 🔴 GC 暂停、内存压力、event loop 阻塞 |
| **子进程管理** | 🟢 std::process + tokio::process、进程组、Job Object (Win) | 🟢 os/exec、context cancellation | 🟡 child_process、信号处理跨平台不一致 |
| **JSONL/JSON-RPC 流** | 🟢 serde_json + tokio::io::BufReader 行流 | 🟢 encoding/json + bufio | 🟢 JSON + readline，最成熟 |
| **Git/文件系统** | 🟢 git2 (libgit2) 或 Command、tokio::fs | 🟡 仅 Command（无 libgit2 绑定） | 🟢 simple-git、fs 最成熟 |
| **跨平台** | 🟢 条件编译，但需平台特定代码 | 🟢 统一标准库 | 🟡 Node.js runtime 屏蔽差异，但 runtime 本身需安装 |
| **单二进制分发** | 🟢 静态链接，~10-20MB | 🟢 静态链接，~10-15MB | 🔴 需 Node.js runtime 或 bun/pkg 编译 (~50MB+) |
| **交互式 CLI/TUI** | 🟢 ratatui + crossterm，生态成熟 | 🟢 bubbletea + lipgloss | 🟢 Ink (React TUI)，但需 Node.js |
| **状态机正确性** | 🟢 enum + exhaustive match，编译期验证 | 🔴 无 sum types，interface 模拟，无编译期穷举 | 🟡 union types + switch，但无运行时穷举保证 |
| **并发正确性** | 🟢 Send/Sync trait，编译期数据竞争检测 | 🟡 goroutine + channel，运行时检测 | 🔴 单线程 event loop，worker_threads 是后加的 |
| **SQLite** | 🟢 rusqlite (libsqlite3-sys) | 🟢 mattn/go-sqlite3 | 🟢 better-sqlite3 |
| **Agent SDK 接入** | 🟡 通过 CLI 子进程（stream-json/JSON-RPC）| 🟡 同左 | 🟢 原生 JS SDK |
| **开发速度** | 🔴 学习曲线、编译时间 | 🟢 快速迭代 | 🟢 最快速 |
| **生态系统** | 🟡 年轻但增长快 | 🟢 成熟 | 🟢 最成熟 |

---

## Decision

**Rust** 作为 Harness Core 和 CLI/TUI 的主要语言。

### Adapter 策略

```
Claude Code    →  ClaudeCliAdapter      (子进程 stream-json, Rust 原生实现)
Codex          →  CodexCliAdapter         (`codex exec --json` 子进程, stdout JSONL, Rust 原生实现)
Fake (测试)    →  FakeAgentAdapter       (Rust 原生实现)
未来扩展        →  ACP Adapter           (子进程 JSON-RPC, Rust 原生实现)
```

### Sidecar 策略

**仅在**某个 SDK 具有不可替代能力时（且该 SDK 无 Rust 绑定），才引入 TypeScript/Python sidecar 进程。Sidecar 通过 stdio JSON-RPC 与 Harness Core 通信。

Foundation Release **不引入任何 Sidecar**。

---

## Consequences

### 正面

- **稳定性**：所有权系统消除内存泄漏、数据竞争和 null pointer 问题——对长时间运行的编排器至关重要
- **进程控制**：`std::process::Command` + `tokio::process` 提供精确的子进程生命周期管理；Windows Job Objects 可管理整个进程树
- **状态机**：Rust enum + exhaustive match 提供编译期保证——非法状态转换在编译时被捕获
- **单二进制**：`cargo build --release` 产出一个静态链接二进制，用户可以 `curl | sh` 或 `scoop install` 安装
- **零运行时依赖**：不需要 Node.js、Python 或任何运行时
- **并发正确性**：Send/Sync trait 防止编译期数据竞争
- **跨平台进程管理**：`#[cfg(windows)]` / `#[cfg(unix)]` 条件编译

### 负面

- **开发速度较慢**：学习曲线陡峭，编译时间较长
- **Agent SDK 不便**：Claude/Codex SDK 均为 TypeScript 优先，但我们的策略是通过 CLI 子进程协议交互，不依赖 SDK API
- **招聘难度**：Rust 开发者池小于 TypeScript/Go
- **迭代速度**：对于实验性的规划/路由算法，Rust 的修改-编译-运行循环比脚本语言慢

### 对 Agent 接入的影响

```
Claude Code 接入：
  ✅ CLI stream-json 协议是稳定的公共接口（非 SDK 私有 API）
  ✅ 参数 --input-format stream-json --output-format stream-json 是文档化的
  ✅ Rust 实现：serde_json 解析 JSONL 行 → 统一 AgentEvent 枚举

Codex 接入：
  ✅ `codex exec --json` stdout JSONL 是稳定的公共接口
  ✅ Rust 实现：serde_json 构建/解析 JSON-RPC 消息
  ⚠️ Codex SDK 原生 TypeScript API 不可用——但使用 JSON-RPC 不损失功能
```

---

## Rejected Alternatives

### TypeScript (原 ADR-001 选择)

拒绝原因：
1. **运行时依赖**：要求用户安装 Node.js 18+（~60-100MB 额外安装）
2. **长时间稳定性**：event loop 阻塞、内存压力、GC 暂停在数小时运行中不可预测
3. **单线程限制**：并发子进程管理依赖异步 I/O，但 CPU 密集型 JSON 解析可能阻塞事件循环
4. **进程树控制**：Node.js `child_process` 的跨平台进程组管理不如 Rust `std::process`
5. **并发正确性**：无编译期数据竞争检测，依赖开发者自律
6. **单二进制分发**：需要 `bun build --compile` 或 `pkg`，产物 50MB+ 且可能引入兼容性问题

### Go

拒绝原因：
1. **无 sum types**：状态机无法用 enum + exhaustive match 实现，只能用 interface + type switch——无法编译期保证穷举
2. **并发资源泄漏**：goroutine 泄漏难以在编译期检测
3. **Git 操作**：无 libgit2 绑定，所有 Git 操作必须通过 CLI（增加脆弱性）
4. **虽然 Go 在"开发速度"上优于 Rust**：但状态机正确性（权重 #8）比开发速度（权重 #11）更重要

---

## Future Migration Cost

如果未来需要迁移到其他语言：
- **Rust → Go**：中等。Rust 的 enum/pattern matching 需要重写为 Go interface pattern
- **Rust → TypeScript**：中等。类型系统可部分对应，但并发模型完全不同
- **保持 Rust**：无迁移成本。Rust 生态系统持续增长

---

## Adapter Sidecar 许可

| 条件 | 是否允许 Sidecar |
|------|:---:|
| Agent SDK 提供 Rust 绑定 | ❌ 不需要 sidecar |
| Agent 同时有 CLI 和 SDK，CLI 可满足需求 | ❌ 不需要 sidecar |
| Agent SDK 具有无可替代的能力（如原生 MCP 集成）且无 Rust 绑定 | ✅ 允许 TypeScript/Python sidecar |
| Sidecar 通过 stdio JSON-RPC 与 Core 通信 | ✅ 必须 |
| Sidecar 的生命周期由 Core 管理（spawn/kill/timeout） | ✅ 必须 |
| Foundation Release 中引入 | ❌ 暂不引入 |

所有 Sidecar 引入必须通过 ADR 批准。
