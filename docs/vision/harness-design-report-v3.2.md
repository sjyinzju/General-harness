# Agent Harness — Foundation Release v3.2 (Architecture Correction Review)

> **版本**: v3.2
> **日期**: 2026-07-15
> **取代**: v3.1 (`harness-design-report-v3.1.md`)
> **修订范围**: Architecture Correction Review — 语言选择、CLI 范围、并行调度、事件模型简化、状态机精简、安全措辞、矛盾修正

---

## v3.1 → v3.2 变更摘要

| # | 变更 | v3.1 | v3.2 |
|---|------|------|------|
| 1 | **实现语言** | TypeScript, 单 package | **Rust**, Cargo workspace 4 crates |
| 2 | **Codex Adapter** | CodexSdkAdapter (TS SDK) | **CodexCliAdapter** (`codex exec --json`, stdout JSONL) |
| 3 | **持久化方案** | 完整 Event Sourcing | **Current-State Tables + Append-Only Events** |
| 4 | **状态数量** | 项目 21 + 任务 17 | 项目 15 + 任务 13 (主 lifecycle)，辅助维度独立 |
| 5 | **Scheduler** | Foundation 串行 | **基础并行**: concurrency slot + lease + 独立 cancellation |
| 6 | **CLI 范围** | 简单命令 | **交互式 CLI Shell**: ratatui, Ctrl+C 语义, detach/re-attach |
| 7 | **安全措辞** | "七层安全边界" | "分层策略控制" — 明确声明无 OS 级沙箱 |
| 8 | **F4 拆分** | 单阶段 | F4a (Contract+Fake Core) + F4b (Fake Integration) |
| 9 | **模块结构** | `src/{domain,application,infrastructure,cli}/` | `crates/{harness-core,harness-runtime,harness-adapters,harness-cli}/` |
| 10 | **"无多 Agent 支持"** | 歧义表述 | 修正为 "不支持任意第三方插件式 Agent" |

---

## 关键决策摘要

### 语言：Rust

选择理由（按权重排序）：
1. 长时间本地进程稳定性（无 GC、所有权系统）
2. 多子进程并发控制（std::process + tokio::process + Job Objects）
3. 单二进制跨平台分发（静态链接，~10-20MB）
4. 状态机编译期穷举（enum + exhaustive match）
5. 并发正确性（Send/Sync trait，编译期数据竞争检测）
6. Claude/Codex 通过 CLI stream-json / JSON-RPC 子进程协议接入（不依赖 SDK 语言绑定）

详见 `docs/architecture/adr/001-core-runtime-language.md`。

### Adapter 策略

```text
Claude Code → ClaudeCliAdapter      (子进程 stream-json)
Codex       → CodexCliAdapter (`codex exec --json` 子进程, stdout JSONL)
Fake        → FakeAgentAdapter      (Rust 原生)
```

仅在 SDK 具有不可替代能力时才引入 TypeScript/Python sidecar。Foundation 不引入任何 sidecar。

### 状态机精简

主生命周期状态（互斥）+ 辅助维度（独立）：

```text
项目: 13 lifecycle + 2 terminal = 15 主状态
      + health: HEALTHY | DEGRADED | STALLED
      + waiting_on: NONE | USER_APPROVAL | USER_FEEDBACK | BLOCKER
      + pause: NONE | PAUSE_REQUESTED | PAUSED | RESUMING
      + reason: null | "agent_unavailable" | ...

任务: 10 lifecycle + 3 terminal = 13 主状态
      + health: HEALTHY | RETRYING | STALLED
      + waiting_on: NONE | SCOPE_EXPANSION | USER_INPUT
      + retry: { count: u32, max: u32 }
      + reason: null | "verification_failed" | ...
```

消失的旧状态 → 辅助维度映射见 `state-machines.md` §7。

### 持久化

采用方案 B：Current-State Tables + Append-Only Events

- `current_state` 表是事实来源
- 每次变更在同一 SQLite 事务中写入 current_state + event_log
- event_log 用于审计和 reconciliation 辅助

### 安全声明

明确说明 Foundation Release 的控制是 policy controls（不是 OS 级隔离），真正沙箱属于 Production Release。

### CLI

Foundation 包含交互式 CLI Shell（ratatui）：
- 流式 AgentEvent 显示
- Ctrl+C 双击语义（第一次暂停/第二次取消）
- Detach 后 Run 继续
- Re-attach 恢复
- 核心不依赖 TUI（通过 HarnessApi trait 抽象）

---

## Foundation Release 能力声明

### ✅ 真实能力

- 发现本地 Claude Code / Codex CLI 并探测能力
- 交互式 CLI Shell（ratatui）
- 基础并行任务调度（concurrency slot + DAG + WorkspaceLease）
- 独立 Git worktree 隔离
- 调用 Agent 子进程执行任务
- 确定性验收 + Harness 独占 git commit
- 分层策略控制（7 层检查，非 OS 级沙箱）
- 崩溃恢复 + reconciliation
- 审计日志
- Golden Path 在 CI 中始终可运行

### ❌ Foundation 不具备

- LLM 驱动的自动规划/审查/修复
- 历史学习路由
- OS 级安全沙箱
- Web UI / 完整 TUI 仪表板
- 任意第三方 Agent 插件
- 分布式/多机执行

---

## 交叉引用

| 主题 | 详细文档 |
|------|---------|
| 语言决策 | `docs/architecture/adr/001-core-runtime-language.md` |
| 持久化+事件 | `docs/architecture/adr/002-sqlite-and-event-model.md` |
| Git 所有权 | `docs/architecture/adr/003-git-operations-owned-by-harness.md` |
| Worktree 绑定 | `docs/architecture/adr/004-worktree-binding-to-task-not-agent.md` |
| 状态机 | `docs/architecture/state-machines.md` |
| 安全 | `docs/architecture/security-boundaries.md` |
| CLI 架构 | `docs/architecture/cli-architecture.md` |
| Adapter 契约 | `docs/architecture/adapter-contract.md` |
| 实施计划 | `docs/implementation/foundation-release-plan.md` |
| 验收标准 | `docs/implementation/foundation-acceptance-criteria.md` |
