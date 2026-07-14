# Foundation Release Risk Register — Agent Harness

> **版本**: v1.0
> **日期**: 2026-07-14

---

## 风险评估矩阵

概率: Low / Medium / High
影响: Low / Medium / High / Critical

---

## 风险清单

| # | 风险 | 概率 | 影响 | 缓解措施 | 残留风险 | 负责人 |
|---|------|------|------|---------|---------|--------|
| R1 | **Agent CLI 协议变更**：Claude/Codex 的 stream-json/JSON-RPC 协议在版本更新后变化 | Medium | High | Adapter 版本检测 + 协议版本协商；contract test 在更新后重新运行 | Medium | Adapter 开发者 |
| R2 | **子进程资源泄漏**：Agent 子进程未正确终止，积累僵尸进程 | Medium | Medium | ProcessManager 维护进程树；SIGKILL 兜底；启动时孤儿进程清理；WorkspaceLease 过期机制 | Low | ProcessManager |
| R3 | **SQLite 并发冲突**：单写入者瓶颈阻碍未来多 Harness 实例 | Low | Low (当前) | Foundation 单进程够用；SQLite WAL 支持并发读；未来可迁移到 PostgreSQL 或引入写入代理 | None (当前) | Persistence |
| R4 | **Git Worktree 残留**：异常退出后 worktree 未清理，占用磁盘 | Medium | Low | reconciliation 检测过期 worktree；定期清理任务（后续版本）；用户可手动清理 | Low | Workspace |
| R5 | **Agent 输出不可解析**：Agent 返回的 TaskResult 不符合 Schema | High | Medium | Schema 校验拒绝；记录原始输出用于调试；Agent 不可用时标记 DEGRADED | Medium | Adapters |
| R6 | **路径逃逸绕过**：Agent 通过符号链接、..、绝对路径绕过 FileScopeValidator | Medium | Critical | 多层防御：路径规范化 + prefix 检查 + .git/.harness 禁止；已知绕过模式加入测试 | Medium | Policy Engine |
| R7 | **密钥泄露**：Agent 生成的代码包含硬编码密钥/Token，SecretScanner 未检测到 | Medium | High | 多层防御：commit 前扫描 + 用户审批 + diff 审查；仅阻止已知模式，零日模式仍有风险 | Medium | Policy Engine |
| R8 | **成本失控**：Agent 在无限循环中消耗大量 API Token | Medium | High | maxTurns 硬限制；maxTime 硬限制；单任务预算；超限自动终止 | Low | Scheduler |
| R9 | **状态不一致**：崩溃后 event_log 与 Git 状态不同步 | Low | Critical | 事务边界设计；reconciliation 修复不一致；不变量检查 | Low | Recovery |
| R10 | **过度设计**：花费过多时间在 Foundation 不需要的功能上 | High | High | 严格的阶段边界；F0-F10 按依赖关系拆分；每次 Review 检查是否超出范围 | Medium | 全体 |
| R11 | **Agent SDK 认证问题**：Claude Agent SDK 与 CLI 登录状态不互通 | Medium | Medium | Foundation 优先 CLI Adapter；ClaudeSdkAdapter 延迟到 Functional Release；如 CLI 登录可驱动 SDK 则提前 | Low | Claude Adapter |
| R12 | **跨平台兼容**：Windows 和 macOS 上的文件路径、进程管理差异 | Medium | Medium | 使用 Node.js 跨平台 API (`path`, `os`)；平台特定代码用条件编译；CI 在 Windows/macOS/Linux 上运行 | Medium | 全体 |
| R13 | **TypeScript 单线程限制**：长时间运行的 Agent 阻塞事件循环 | Low | Medium | Agent 在子进程中运行（异步 I/O）；Harness 主进程只做协调；worker_threads 备用方案 | Low | ProcessManager |
| R14 | **用户直接修改 worktree**：用户在 Agent 执行期间手动编辑 worktree 文件 | Low | Medium | WorkspaceLease 警告；diff 检查时发现意外变更 → 标记；文档告知用户不要手动操作 | Medium | UX/Docs |
| R15 | **网络不稳定**：Agent CLI 调用远程 API 时网络故障 | Medium | Medium | 超时机制；Agent 自身重试；超限后 Harness 标记 FAILED_RETRYABLE → 换 Agent/重试 | Medium | Adapters |

---

## 不可缓解的风险（已知接受）

| # | 风险 | 接受原因 |
|---|------|---------|
| A1 | Foundation Release 不提供 OS 级沙箱 | 实现成本太高（容器/VM），推迟到 Production Release；文档如实描述限制 |
| A2 | Agent 可能产生有 bug 的代码通过验收 | 验收条件由用户定义；单元测试覆盖率不是 100%；LLM 代码生成本质上非确定性 |
| A3 | 首次运行需要用户手动安装和认证 Agent CLI | 这是 Agent CLI 的固有要求，Harness 无法绕过；配置向导提供清晰指引 |

---

## 风险审查日程

| 阶段 | 审查重点 |
|------|---------|
| F2 完成后 | R9（状态一致性）、R4（worktree 残留） |
| F5 完成后 | R1（协议变更）、R5（输出不可解析）、R11（SDK 认证） |
| F7 完成后 | R6（路径逃逸）、R7（密钥泄露）、R8（成本失控） |
| F9 完成后 | R12（跨平台）、R14（用户干扰）、R15（网络） |
| F10 完成后（发布前） | 全量风险审查，R10（过度设计）评估 |

---

## 风险缓解措施实现状态

| 缓解措施 | 状态 | 阶段 |
|---------|------|------|
| Adapter 版本检测 | 未实现 | F5 |
| ProcessManager 进程树管理 | 未实现 | F3 |
| SQLite WAL 模式 | 未实现 | F2 |
| reconciliation 过期 worktree 检测 | 未实现 | F9 |
| TaskResult Schema 校验 | 未实现 | F1 |
| 路径规范化 + prefix 检查 | 未实现 | F3 |
| commit 前密钥扫描 | 未实现 | F7 |
| maxTurns + maxTime 硬限制 | 未实现 | F3 |
| 不变量检查 | 未实现 | F9 |
| CI 跨平台矩阵 | 未实现 | F0 |
