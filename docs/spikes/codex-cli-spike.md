# Codex CLI Spike Report v2

> **日期**: 2026-07-15
> **版本**: Codex CLI 0.116.0
> **平台**: Windows 11
> **状态**: PARTIAL — 真实 JSONL 事件已捕获, 但模型运行未完成

---

## 1. 配置阻塞诊断

### 原因

`~/.codex/config.toml` 中存在两个与当前 Codex CLI 0.116.0 不兼容的配置项:

| 配置项 | 当前值 | 期望值 | 影响 |
|--------|--------|--------|------|
| `service_tier` | `"default"` | `"fast"` 或 `"flex"` | 阻止 `codex login status` 和 `codex exec` |
| `models_cache` 中的 effort 级别 | `"max"` | `"none"`, `"minimal"`, `"low"`, `"medium"`, `"high"`, `"xhigh"` | 警告，不阻塞 |
| `huashu-design` skill description | >1024 chars | ≤1024 chars | 警告，不阻塞 |

### 绕过方案

**不需要修改用户全局配置**。使用 `codex exec -c <key=value>` 命令行覆盖:

```bash
codex -c service_tier="fast" exec --json ...
```

### 如果必须修改全局配置

| 项目 | 值 |
|------|-----|
| 文件 | `~/.codex/config.toml` |
| 字段 | `service_tier` |
| 当前值 | `"default"` |
| 修改为 | `"fast"` |
| 回滚方法 | 还原为 `"default"` |
| 影响 | 无 — 仅修改 CLI 配置，不影响 Codex 账户或 API Key |

---

## 2. 已捕获的真实 JSONL 事件

### 事件类型

| type | 字段 | 示例值 |
|------|------|--------|
| `thread.started` | `thread_id` | `"019f6439-bb5b-7c82-b0bc-67d34d35d160"` (UUIDv7) |
| `turn.started` | — | 标记 turn 开始 |
| `error` | `message` | `"Reconnecting... 2/5 (timeout waiting for child process to exit)"` |
| `item.completed` | `item.id`, `item.type`, `item.message` | `"item_0"`, `"error"`, `"Falling back from WebSockets..."` |
| `turn.failed` | `error.message` | `"{\"detail\":\"...\"}"` |

### 与 Claude stream-json 的关键差异

| 维度 | Claude | Codex |
|------|--------|-------|
| Session 标识 | `session_id` (UUIDv4) | `thread_id` (UUIDv7) |
| Turn 标记 | 隐式 (assistant+result 流) | 显式 `turn.started` / `turn.failed` |
| Streaming | assistant 块流式更新 | item delta 事件 |
| 工具事件 | `content[].tool_use` / `tool_result` | ❓ (本 spike 未捕获到成功执行) |
| 最终结果 | `result` event | ❓ (预期: `turn.completed`) |
| 错误 | `result` with `is_error:true` | 独立 `error` event + `turn.failed` |
| Item 生命周期 | 无对应概念 | `item.completed` (每个工具调用/item) |

---

## 3. 验证结果

| # | 检查项 | 结果 | 备注 |
|---|--------|:---:|------|
| 1 | `codex --version` | ✅ | `codex-cli 0.116.0` |
| 2 | `codex login status` | ✅ | `Logged in using ChatGPT` (需要 `-c service_tier="fast"`) |
| 3 | `codex exec --help` | ✅ | JSONL, `--json`, `--cd`, `--full-auto`, `-c` 均可用 |
| 4 | `codex exec --json` | ✅ | JSONL 在 stdout, stderr 独立采集 |
| 5 | 最小写文件任务 | ❌ | 模型 `gpt-5.5` 需要更新 Codex, `gpt-4` 被分类器阻塞 |
| 6 | stdout JSONL | ✅ | 每行一个 JSON 对象 |
| 7 | stderr | ✅ | 技能加载错误 + 日志行 (非 JSONL) |
| 8 | thread_id | ✅ | UUIDv7 格式: `019f6439-...` |
| 9 | 正常结果 | ❓ | 预期 `turn.completed`, 未捕获 |
| 10 | 失败结果 | ✅ | `turn.failed` + `error.message` |
| 11 | Structured output | ❓ | `--output-schema` flag 存在, 未验证 |
| 12 | Timeout | ✅ (间接) | Reconnect timeout 机制已验证 |
| 13 | Cancellation | ❓ | 未测试 (需运行中的进程) |
| 14 | Native resume | ❓ | `codex exec resume` 子命令存在, 未验证 |
| 15 | 权限不足 | ❓ | `--sandbox` 选项可用, 未测试 |
| 16 | Unknown event | ❓ | 所有事件均映射到已知类型 |
| 17 | Malformed line | ❓ | 未触发 |
| 18 | 非零 exit code | ✅ | `exit code 1` 在 API 错误时 |

---

## 4. AgentEvent 映射 (基于真实数据修订)

| Codex JSONL | AgentEvent | 确认程度 |
|------------|------------|:---:|
| `thread.started` | `SessionStarted { session_id: thread_id }` | ✅ 已确认 |
| `turn.started` | *(internal — 不映射)* | ✅ |
| `item.completed` (assistant message) | `Message { content }` | ❓ 预期 |
| `item.completed` (tool_use) | `ToolCallStarted` | ❓ 预期 |
| `item.completed` (tool_result) | `ToolCallCompleted` | ❓ 预期 |
| `turn.completed` | `Result { is_error: false }` | ❓ 预期 |
| `error` | `Error { message }` | ✅ 已确认 |
| `turn.failed` | `Result { is_error: true }` | ✅ 已确认 |
| Process exit | `ProcessExited { exit_code }` | ✅ |
| Unknown event type | `RawVendorEvent` | ✅ |

---

## 5. 阻塞项状态

| 阻塞 | 状态 | 解决方案 |
|------|:---:|------|
| config.toml 阻塞 | ✅ 已绕过 | `-c service_tier="fast"` 命令行覆盖 |
| gpt-5.5 模型不可用 | ⚠️ | 需要 `-m gpt-4` 或升级 Codex CLI |
| 写文件任务未完成 | ⚠️ | 待分类器可用后重试 |
