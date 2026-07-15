# Test Strategy v3 — Agent Harness

> **版本**: v3.0
> **日期**: 2026-07-15
> **修订**: Gate-aligned 测试, Resource Claim 测试, LOST recovery 测试

---

## 1. 测试与 Gate 对齐

| Gate | 测试重点 |
|------|---------|
| Gate A | `cargo check`, `cargo fmt`, `cargo clippy`, FSM 穷举测试 |
| Gate B | FakeAdapter contract tests, 单任务 Golden Path |
| Gate B→C | Tech spike 验证: Codex/Claude 真实事件映射 |
| Gate C | Contract freeze: 所有 adapter 通过 contract suite |
| Gate D | Full Golden Path matrix (Fake+Real), parallel+crash |
| Gate E | Foundation acceptance, Real adapter conditional |

---

## 2. Golden Path Matrix

| 场景 | FakeAdapter | CodexCliAdapter | ClaudeCliAdapter |
|------|:---:|:---:|:---:|
| 单任务成功 | ✅ CI always | 条件 | 条件 |
| 两任务并行成功 | ✅ CI always | 条件 | 条件 |
| 一成功一失败 + 下游 SUPERSEDED | ✅ CI always | — | — |
| 取消一任务不影响另一 | ✅ CI always | — | — |
| Resource Claim 冲突 → 串行 | ✅ CI always | — | — |
| Supervisor crash → LOST → retry --resume | ✅ CI always | — | — |
| Agent unavailable → DEGRADED | ✅ CI always | 条件 | 条件 |

---

## 3. 新增测试场景

### Resource Claim

```rust
#[tokio::test]
async fn write_write_conflict_serializes() {
    // Two tasks claim same file WRITE → only one dispatched
}

#[tokio::test]
async fn read_read_compatible_concurrent() {
    // Two tasks claim same file READ → both dispatched
}
```

### LOST Recovery

```rust
#[tokio::test]
async fn supervisor_crash_execution_lost_retry_with_resume() {
    // Kill supervisor → Execution LOST
    // New supervisor → reconciliation → new Execution + --resume
}
```

### Downstream Cancellation

```rust
#[tokio::test]
async fn upstream_cancelled_downstream_superseded() {
    // Task A CANCELLED → Task B (depends on A) → SUPERSEDED
}
```

---

## 4. CI 矩阵

```yaml
os: [ubuntu-latest, windows-latest, macos-latest]
rust: [stable]
test: [unit, integration (FakeAdapter)]
conditional: [codex, claude]  # 仅当 env RUN_REAL_TESTS=1
```
