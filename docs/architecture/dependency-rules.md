# Dependency Rules (Revised) — Agent Harness

> **版本**: v2.0
> **日期**: 2026-07-15
> **修订**: Rust Cargo workspace crates 依赖规则

---

## 1. Crate 依赖图

```
testing-kit ──→ harness-core ←── harness-runtime ←── harness-cli
                    ↑                  ↑
                    │                  │
            harness-adapters ──────────┘
```

## 2. 规则

### Rule 1: harness-core 零外部依赖

`harness-core` 只能依赖：
- `serde`, `serde_json` (序列化)
- `chrono` (时间类型)
- `uuid` (ID 生成)
- `thiserror` (错误类型)
- `async_trait` (异步 trait)

**禁止**依赖：`rusqlite`, `tokio` (仅 `async_trait` 宏), `git2`, `ratatui`, `crossterm`

### Rule 2: Adapter 只通过 Trait 引用

`harness-runtime` 通过 `AgentAdapter` trait 使用 Adapter。**禁止** import 具体 Adapter struct。

### Rule 3: 无循环依赖

Cargo 编译检查自动防止。PR CI 中不需要额外工具。

### Rule 4: 无字符串分发

**禁止**通过 agent/adapter 名称字符串做 `match` / `if-else`：

```rust
// ❌ 禁止
match profile.agent_kind.as_str() {
    "claude-code" => { /* special */ }
    "codex" => { /* special */ }
    _ => {}
}

// ✅ 正确：通过 AdapterRegistry + AgentAdapter trait 多态
let adapter = registry.get(&profile.adapter_kind)?;
adapter.start_session(&profile, &opts).await
```

唯一例外：Adapter 工厂和 AgentDiscoveryService。

### Rule 5: 无隐式全局状态

- 所有状态通过显式参数或依赖注入传递
- 禁止 `lazy_static!` / `once_cell::sync::Lazy` 保存可变状态
- 配置对象通过 `Arc<Config>` 在初始化时注入

### Rule 6: Schema 边界

- 所有跨 crate 的数据类型实现 `Serialize + Deserialize`
- 从 Agent 子进程接收的 JSON 必须经过 `serde_json::from_str::<T>()` 校验
- 从 SQLite 读取的 JSON 字段必须经过反序列化校验
- 使用 `#[serde(deny_unknown_fields)]` 防止未识别的字段静默通过

### Rule 7: Trait 必须有调用方

- 每个 `pub trait` 至少有一个实现 + 一个调用方 **或** 在 testing-kit 中有 contract test
- 每个 `pub fn` 至少被一个测试调用
- **禁止** "为未来准备的" trait 方法

---

## 3. CI 强制

```toml
# 通过 Cargo.toml 的 [dependencies] 自动强制执行依赖规则
# harness-core/Cargo.toml — 不包含 rusqlite, tokio, git2
# harness-runtime/Cargo.toml — 不包含 harness-adapters, harness-cli
```

```yaml
# CI: 检查无用依赖
- name: Check unused dependencies
  run: cargo udeps --all-targets

# CI: 检查循环依赖 (Cargo 编译时自动检测)
- name: Build check
  run: cargo check --workspace
```

---

## 4. 例外登记

| 例外 | 条件 | ADR |
|------|------|-----|
| Adapter 工厂中 `match adapter_kind` | 仅在 `harness-adapters/src/mod.rs` 注册处 | ADR-004 |
| `AgentDiscoveryService` 调用具体检测方法 | 发现阶段的特例 | ADR-004 |
| Sidecar 子进程（未来） | 需要 TypeScript/Python runtime 时 | 需要新 ADR |

---

## 5. 未来 Crate 添加规则

添加第 5+ 个 crate 前：
1. 写 ADR 说明为什么不能放在现有 4 个 crate 中
2. 明确依赖方向（依赖 haram-core 和/或 harness-runtime）
3. 检查不会引入循环依赖
4. Foundation Release 中不允许超过 6 个 crate
