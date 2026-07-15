# Foundation Risk Register v3

> **版本**: v3.0
> **日期**: 2026-07-15
> **修订**: 新增 Tech Spike 风险, Operation/Saga 复杂度风险

---

## 新增/修订风险

| # | 风险 | P | I | 缓解 |
|---|------|---|---|------|
| R16 | **Tech Spike 发现 AgentEvent v2 不足** — Codex/Claude 的事件无法完整映射 | M | H | Gate B→C 之间 spike; 如果不足则修订 AgentEvent v2 再 freeze |
| R17 | **--resume 不可靠** — Claude/Codex 的 resume 在真实 LOST 场景下行为不符合预期 | M | H | Spike 验证; 如果不可靠则 LOST 场景仅支持全新重试 (无 resume) |
| R18 | **Operation/Saga 复杂度** — reconciliation 逻辑 bug 导致状态不一致 | M | H | 穷举 reconciliation 测试; 简单状态机 |
| R19 | **IPC socket 跨平台** — Windows named pipe 权限/路径问题 | M | M | 早期 CI matrix; Windows 环境测试 |
| R20 | **Watchdog 在 Windows 上不可靠** — Job Object 需要特定 API 权限 | L | M | 优先 Job Object; fallback: toolhelp snapshot + TerminateProcess |

## 已消除风险

| 旧风险 | 消除原因 |
|--------|---------|
| "TUI as supervisor 线程" | 改为独立 headless supervisor |
| "重新接管 Agent pipe" | 明确不可行 → LOST + 新 Execution |
| "外部副作用在事务前" | 改为 Operation/Saga |
| "无 Resource Claim" | Foundation 实现基础 Resource Claim |
| "空 commit 允许" | 明确禁止 |

## Tech Spike 重点验证

Gate B → C 之间必须验证:

1. Codex CLI JSON-RPC → AgentEvent v2 映射完整性
2. Claude CLI stream-json → AgentEvent v2 映射完整性
3. `--resume` (Claude) 跨进程可靠性
4. Codex thread resume 跨进程可靠性
5. RawVendorEvent 是否真的需要 (如果所有事件都已映射) 或是否有遗漏
