# Test Strategy — Agent Harness

> **版本**: v1.0
> **日期**: 2026-07-14

---

## 1. 测试金字塔

```
                    ┌──────────────┐
                    │   E2E Tests   │  Golden Path (FakeAdapter, always in CI)
                    │   (少量)       │
                    ├──────────────┤
                    │  Integration  │  Real Adapter tests (conditional)
                    │  Tests        │  CLI integration tests
                    ├──────────────┤
                    │  Unit Tests   │  Domain logic, state machine,
                    │  (大量)       │  policy engine, command handlers
                    └──────────────┘
```

---

## 2. 测试层次

### 2.1 Unit Tests

| 测试对象 | 覆盖内容 | 运行环境 |
|---------|---------|---------|
| `domain/contracts/` | Schema 校验（Zod） | Node.js |
| `domain/state-machine/` | 所有合法转换、所有非法转换拒绝、前置条件、幂等 | Node.js |
| `domain/policies/` | 策略规则逻辑 | Node.js |
| `application/commands/` | Command Handler 逻辑（mock event store） | Node.js |
| `application/services/` | Scheduler、Verifier、Reconciliation 逻辑 | Node.js |
| `infrastructure/policy-engine/` | 命令过滤、路径验证、密钥扫描 | Node.js |

**单元测试不依赖**：文件系统、SQLite、子进程、真实 Git 仓库。

### 2.2 Integration Tests

| 测试对象 | 覆盖内容 | 环境要求 |
|---------|---------|---------|
| SQLite Event Store | 写入、读取、projection、migration | 文件系统 |
| Worktree Manager | 创建/清理 worktree | Git + 文件系统 |
| Process Manager | 子进程启动/终止/超时 | OS 进程 |
| FakeAgentAdapter | 完整 session 生命周期 | 无 |
| CLI Commands | 端到端命令行行为 | 文件系统 + SQLite |

### 2.3 E2E Tests (Golden Path)

| Path | Adapter | 环境 | CI 运行？ |
|------|---------|------|:---:|
| Golden Path — FakeAdapter | Fake | 无 | ✅ 始终 |
| Golden Path — CodexSdkAdapter | Codex | Codex CLI 已安装且认证 | 条件（环境变量开关） |
| Golden Path — ClaudeCliAdapter | Claude | Claude CLI 已安装且登录 | 条件（环境变量开关） |
| Crash Recovery — FakeAdapter | Fake | 无 | ✅ 始终 |
| Agent Unavailable — FakeAdapter | Fake | 无 | ✅ 始终 |

---

## 3. 关键测试原则

### 3.1 状态机测试

- **穷举合法转换**：对每个 `(from, to)` 合法对，验证 transition 成功
- **抽样非法转换**：对每种非法转换类别验证被拒绝
- **前置条件**：验证每个转换的前置条件缺失时被拒绝
- **幂等**：验证相同 idempotencyKey 的重复调用返回相同结果

### 3.2 Adapter Contract Tests

详见 `docs/testing/adapter-contract-tests.md`。

每个 Adapter 实现必须通过共享的 contract test suite。

### 3.3 确定性原则

- 所有测试必须是**确定性的**（相同输入 → 相同结果）
- 不依赖时间戳（使用固定时间或注入 clock）
- 不依赖随机数
- FakeAdapter 提供完全可控制的 Agent 行为

---

## 4. 测试工具

| 工具 | 用途 |
|------|------|
| `vitest` | 测试运行器 |
| `better-sqlite3` (in-memory) | 单元测试中的数据库模拟 |
| `memfs` / `tmp` | 单元测试中的文件系统模拟 |
| `FakeAgentAdapter` | Agent 行为的完全控制 |
| `TestFixtures` | 共享测试数据（Project、Task、Profile 实例） |

---

## 5. CI 中的测试策略

```
PR 触发:
  ✅ lint + typecheck
  ✅ unit tests (vitest, ~5s)
  ✅ state machine exhaustive tests
  ✅ FakeAdapter Golden Path E2E

Main 分支合并:
  ✅ 以上全部
  ✅ Real Adapter integration tests (条件性)
  ✅ Coverage check (>80% for domain/)

发布前:
  ✅ 以上全部
  ✅ Full Golden Path matrix
```

---

## 6. 不可测试的场景

以下场景**无法在 CI 中自动测试**，需要手动验证：

| 场景 | 原因 |
|------|------|
| 真实 Claude Code CLI 交互 | 需要 Anthropic 账号和 API 额度 |
| 真实 Codex CLI 交互 | 需要 OpenAI 账号和 API 额度 |
| 系统重启恢复 | 需要 OS 级别控制 |
| 跨平台兼容 | 需要在 Windows/macOS/Linux 物理机上测试 |
| 长时间运行稳定性 | 需要数小时的运行时间 |
