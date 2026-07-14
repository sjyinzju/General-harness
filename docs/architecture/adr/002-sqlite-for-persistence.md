# ADR-002: SQLite for Persistence

> **状态**: Accepted
> **日期**: 2026-07-14

---

## Context

Harness 需要持久化项目状态、事件日志、审计轨迹。候选：SQLite、PostgreSQL、纯文件系统（JSON/JSONL）。

## Decision

使用 **SQLite**，通过 `better-sqlite3` 库。

## Rationale

1. **零配置**：无需安装数据库服务器，用户只需 `npm install -g`
2. **嵌入式**：单个文件，方便备份和迁移
3. **WAL 模式**：提供 crash-safe 保证，支持并发读
4. **单写入者**：Harness 单进程模型与 SQLite 单写入者天然匹配
5. **事务**：ACID 保证，Command → Event → Projection 可在一个事务中

## Consequences

- **正面**：极简部署，零运维
- **正面**：WAL 模式保证崩溃不丢已提交数据
- **正面**：可以 SQL 查询直接调试
- **负面**：不支持网络访问（如果未来需要分布式）
- **负面**：并发写入受限于单写入者（当前单进程模型下不是问题）
- **负面**：未来迁移到其他数据库需要重写 persistence 层

## Alternatives Considered

- **PostgreSQL**：功能强大但需要用户安装和配置，不适合"本地工具"定位
- **纯 JSON 文件**：简单但无事务、无查询、崩溃不安全
- **JSONL (append-only)**：崩溃安全但查询效率极低，缺乏 ACID
