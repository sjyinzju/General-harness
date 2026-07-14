# ADR-001: TypeScript as Primary Language

> **状态**: Accepted
> **日期**: 2026-07-14

---

## Context

需要选择 Agent Harness 的主要开发语言。候选：TypeScript、Rust、Python、Go。

## Decision

使用 **TypeScript**。

## Rationale

1. **与 Agent SDK 同生态**：Claude Agent SDK 和 Codex SDK 均为 TypeScript/JavaScript
2. **单线程事件循环天然适合编排**：子进程管理、流式事件处理、异步协调
3. **Zod**：提供编译时类型推断 + 运行时 Schema 校验
4. **开发速度**：相比 Rust 更快迭代，Foundation Release 不需要极致性能
5. **跨平台**：Node.js 在 Windows/macOS/Linux 上均可运行

## Consequences

- **正面**：快速的开发迭代、与 Agent SDK 无阻抗
- **正面**：Zod 提供完整的输入验证链路
- **负面**：单线程模型下 CPU 密集型操作需谨慎处理
- **负面**：进程崩溃会导致所有活跃任务需要 reconciliation
- **负面**：长期运行的内存管理需要关注（相比 Rust 无编译期保障）

## Alternatives Considered

- **Rust**（参考 Galcode_island）：子进程管理更安全，但开发速度慢，与 JS SDK 有 FFI 障碍
- **Python**：AI/ML 生态好，但类型系统弱，异步模型不如 Node.js 成熟
- **Go**：并发好，但 Agent SDK 生态几乎为零
