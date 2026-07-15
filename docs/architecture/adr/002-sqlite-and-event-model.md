# ADR-002 (Revised): Persistence — Current-State + Operation/Saga

> **状态**: Accepted
> **日期**: 2026-07-15
> **取代**: 旧 ADR-002 v1.0
> **修订**: 增加 Operation/Saga 两阶段模型, 移除"零迁移 ES"承诺

---

## Context

Harness 需要持久化项目状态和执行副作用操作 (git commit, merge, etc.)。需要处理数据库事务和外部副作用之间的一致性问题。

原 ADR-002 选择 Current-State Tables + Append-Only Events (Scheme B)，但没有解决外部副作用一致性。

## Decision

**Current-State Tables + Append-Only Events + Operation/Saga 模型**

### Operation/Saga 补充

所有外部副作用 (git, filesystem) 通过三阶段 Operation 模型:

```
Phase 1 (SQLite 事务): INSERT operation (PENDING) + INSERT event_log
Phase 2 (外部): 执行副作用 (git commit, etc.) — operation_id 写入 git trailer
Phase 3 (SQLite 事务): UPDATE operation (COMPLETED/FAILED) + INSERT event_log
```

Reconciliation 处理长期 PENDING/RUNNING 的 operations。

### 不承诺未来零迁移

未来如果需要完整 Event Sourcing，需要:
1. 验证 event_log 完整性
2. 补充缺失的操作事件
3. 解决 event schema versioning
4. **这不是零迁移**

## Consequences

- **正面**: 数据库和外部副作用的一致性由 reconciliation 保证
- **正面**: operation_id 在 git trailer 中提供跨数据库/Git 的关联
- **负面**: reconciliation 逻辑增加了恢复模块的复杂度
- **负面**: 不再承诺零迁移到完整 ES
