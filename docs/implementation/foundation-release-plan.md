# Foundation Release Plan v3 — Agent Harness

> **版本**: v3.0
> **日期**: 2026-07-15
> **修订**: Gate A-E 对应 F0-F10

---

## Gate → Phase 映射

```
Gate A (Architecture Ready)
  → F0: Cargo workspace + CI skeleton
  → F1: Domain contracts (Candidate)
  → F2: State machine + persistence skeleton
Gate A 退出: 所有 type+trait Candidate 完成, 穷举测试通过

Gate B (Contract Candidate)
  → F3: Process + Policy + Workspace + Git (基础)
  → F4a: Adapter Contract + FakeAdapter Core
  → F4b: Fake Execution Integration (单任务 Golden Path)
Gate B 退出: FakeAdapter Golden Path 通过

Tech Spike: Codex CLI + Claude CLI 真实验证
  → 验证 AgentEvent v2 覆盖
  → 验证 --resume 机制

Gate C (Contract Freeze v1)
  → 冻结: AgentAdapter v1, AgentEvent v1, SQLite schema v1
  → F5: CodexCliAdapter + ClaudeCliAdapter 完整实现
  → F6: Discovery + Probe

Gate D (Runtime Integration)
  → F7: Scheduler + DAG + Verification + Commit + Resource Claim
  → F8: CLI + IPC protocol
Gate D 退出: 全部 Golden Path 通过 (Fake + 至少一个 Real)

Gate E (Foundation Acceptance)
  → F9: Golden Path matrix + Recovery
  → F10: Docs + Risk review + v0.1.0

详见: readiness-audit.md §9
```

---

## 关键依赖

```
F0 ──→ F1 ──→ F2 ──→ F3 ──→ F4a ──→ F4b ──→ [Tech Spike]
                                            │
              ┌─────────────────────────────┘
              ▼
            Gate C → F5 → F6 → F7 → F8 → F9 → F10
```

Gate A 通过前不开始 F3-F10 的生产代码。
Gate C 通过前不冻结 Adapter 契约。
