# Container & Module Design (Revised) — Agent Harness

> **版本**: v2.0
> **日期**: 2026-07-15
> **修订**: Rust 实现，Cargo workspace crates，替换单 TypeScript package

---

## 1. Cargo Workspace 结构

```
harness/
├── Cargo.toml                    # workspace root
├── crates/
│   ├── harness-core/             # 领域模型 + 契约
│   │   ├── Cargo.toml            # 依赖: serde, serde_json, chrono, uuid
│   │   └── src/
│   │       ├── contracts/        # 接口 & 类型
│   │       │   ├── mod.rs
│   │       │   ├── agent_adapter.rs
│   │       │   ├── runtime_profile.rs
│   │       │   ├── task_envelope.rs
│   │       │   ├── task_result.rs
│   │       │   ├── goal_contract.rs
│   │       │   ├── project.rs
│   │       │   ├── task.rs
│   │       │   ├── workspace.rs
│   │       │   ├── agent_event.rs
│   │       │   ├── domain_event.rs
│   │       │   └── acceptance_check.rs
│   │       ├── state_machine/    # 状态机（纯函数）
│   │       │   ├── mod.rs
│   │       │   ├── project_fsm.rs
│   │       │   ├── task_fsm.rs
│   │       │   └── transition_rules.rs
│   │       └── policies/         # 策略类型
│   │           ├── mod.rs
│   │           ├── budget.rs
│   │           ├── command.rs
│   │           └── file_scope.rs
│   │
│   ├── harness-runtime/          # 应用 + 基础设施
│   │   ├── Cargo.toml            # 依赖: harness-core, rusqlite, tokio, git2
│   │   └── src/
│   │       ├── persistence/      # SQLite (current-state + event log)
│   │       │   ├── mod.rs
│   │       │   ├── connection.rs
│   │       │   ├── event_store.rs
│   │       │   ├── project_repo.rs
│   │       │   ├── task_repo.rs
│   │       │   ├── profile_repo.rs
│   │       │   ├── audit_store.rs
│   │       │   └── migrations/
│   │       ├── scheduler/        # DAG + 调度
│   │       │   ├── mod.rs
│   │       │   ├── dag.rs
│   │       │   └── dispatcher.rs
│   │       ├── process/          # 子进程管理
│   │       │   ├── mod.rs
│   │       │   ├── manager.rs
│   │       │   └── cancellation.rs
│   │       ├── workspace/        # Git worktree
│   │       │   ├── mod.rs
│   │       │   ├── worktree.rs
│   │       │   ├── lease.rs
│   │       │   └── git_inspector.rs
│   │       ├── verification/     # 验收
│   │       │   ├── mod.rs
│   │       │   ├── checks.rs
│   │       │   └── diff_inspector.rs
│   │       ├── policy_engine/    # 策略执行
│   │       │   ├── mod.rs
│   │       │   ├── command_filter.rs
│   │       │   ├── path_validator.rs
│   │       │   └── secret_scanner.rs
│   │       ├── recovery/         # 崩溃恢复
│   │       │   ├── mod.rs
│   │       │   └── reconciliation.rs
│   │       ├── checkpoint/       # 检查点
│   │       │   └── mod.rs
│   │       └── logging/          # 结构化日志
│   │           └── mod.rs
│   │
│   ├── harness-adapters/         # Agent Adapter 实现
│   │   ├── Cargo.toml            # 依赖: harness-core, tokio, serde_json
│   │   └── src/
│   │       ├── mod.rs            # AdapterRegistry
│   │       ├── fake/             # FakeAgentAdapter
│   │       │   ├── mod.rs
│   │       │   └── adapter.rs
│   │       ├── claude_cli/       # ClaudeCliAdapter
│   │       │   ├── mod.rs
│   │       │   ├── adapter.rs
│   │       │   └── stream_json.rs
│   │       ├── codex_cli/        # CodexCliAdapter
│   │       │   ├── mod.rs
│   │       │   ├── adapter.rs
│   │       │   └── jsonl.rs
│   │       └── discovery/        # AgentDiscoveryService
│   │           ├── mod.rs
│   │           ├── scanner.rs
│   │           └── probe.rs
│   │
│   ├── harness-cli/              # CLI + Interactive Shell
│   │   ├── Cargo.toml            # 依赖: harness-runtime, harness-adapters, ratatui, crossterm
│   │   └── src/
│   │       ├── main.rs           # 入口点
│   │       ├── commands/         # 命令处理器
│   │       │   ├── mod.rs
│   │       │   ├── run.rs
│   │       │   ├── attach.rs
│   │       │   ├── status.rs
│   │       │   ├── approve.rs
│   │       │   ├── pause.rs
│   │       │   ├── resume.rs
│   │       │   ├── cancel.rs
│   │       │   └── config.rs
│   │       ├── interactive/      # 交互式 Shell
│   │       │   ├── mod.rs
│   │       │   ├── app.rs
│   │       │   ├── event_loop.rs
│   │       │   └── views/
│   │       │       ├── mod.rs
│   │       │       ├── status_bar.rs
│   │       │       ├── task_list.rs
│   │       │       └── agent_panel.rs
│   │       └── output.rs         # 终端格式化
│   │
│   └── testing-kit/              # 测试工具包
│       ├── Cargo.toml            # 依赖: harness-core
│       └── src/
│           ├── lib.rs
│           ├── adapter_contract_test.rs  # 可复用的 Adapter 契约测试
│           ├── fake_agent_factory.rs
│           └── test_fixtures.rs
│
├── tests/                        # 集成 & E2E 测试
│   ├── integration/
│   │   ├── golden_path_fake.rs
│   │   ├── golden_path_parallel.rs
│   │   ├── crash_recovery.rs
│   │   └── agent_unavailable.rs
│   └── contract/
│       └── adapter_contract_suite.rs
│
└── docs/                         # 规划文档（已存在）
```

---

## 2. Crate 依赖方向

```
testing-kit ──→ harness-core ←── harness-runtime ←── harness-cli
                    ↑                  ↑
                    │                  │
            harness-adapters ──────────┘
```

### 严格规则

```
✅ harness-core: 零外部依赖（仅 serde + uuid + chrono + thiserror）
✅ harness-runtime: 依赖 harness-core + rusqlite + tokio + git2
✅ harness-adapters: 依赖 harness-core + tokio
✅ harness-cli: 依赖 harness-runtime + harness-adapters + ratatui + crossterm
✅ testing-kit: 仅依赖 harness-core

❌ harness-core 禁止依赖 harness-runtime / harness-adapters / harness-cli
❌ harness-runtime 禁止依赖 harness-adapters / harness-cli
❌ harness-adapters 禁止依赖 harness-runtime / harness-cli
❌ 禁止循环依赖
```

---

## 3. Crate 职责边界

### harness-core

- 所有领域类型与接口（struct、enum、trait）
- 状态机纯函数（`ProjectFsm::can_transition()`, `TaskFsm::can_transition()`）
- 策略类型定义
- **不依赖**：SQLite、Git、文件系统、子进程、TUI、任何 Agent

### harness-runtime

- SQLite 持久化（current_state + event_log + audit_log）
- Scheduler（DAG 拓扑 + 并发控制）
- ProcessManager（子进程生命周期）
- WorktreeManager + WorkspaceLease
- VerificationService + DiffInspector
- PolicyEngine（命令过滤、路径验证、密钥扫描）
- Reconciliation（崩溃恢复）
- Checkpoint
- 通过 `AgentAdapter` trait 接口使用 Adapter（不直接依赖具体 Adapter 实现）

### harness-adapters

- FakeAgentAdapter
- ClaudeCliAdapter（stream-json 子进程）
- CodexCliAdapter（`codex exec --json` 子进程, stdout JSONL）
- AgentDiscoveryService
- AdapterRegistry
- 不操作数据库、不管理 worktree、不执行验证

### harness-cli

- 所有 CLI 命令
- 交互式 Shell (ratatui)
- HarnessApi trait 的实现（将 API 调用委托给 harness-runtime）
- 终端输出格式化和日志显示

---

## 4. 关键接口（trait）

```rust
// harness-core: Agent Adapter 契约
#[async_trait]
pub trait AgentAdapter: Send + Sync {
    fn kind(&self) -> &'static str;
    async fn detect(&self, binary_path: Option<&Path>) -> Result<DetectionResult>;
    async fn get_version(&self) -> Result<String>;
    async fn inspect_configuration(&self) -> Result<AgentConfigInfo>;
    async fn check_authentication(&self) -> Result<AuthCheckResult>;
    async fn probe(&self, temp_dir: &Path) -> Result<ProbeResult>;
    async fn start_session(&self, profile: &RuntimeProfile, opts: &SessionOptions) -> Result<Box<dyn AgentSession>>;
}

#[async_trait]
pub trait AgentSession: Send {
    fn session_id(&self) -> &str;
    fn is_active(&self) -> bool;
    async fn send_task(&mut self, envelope: &TaskEnvelope) -> Result<()>;
    async fn receive_events(&mut self) -> Result<mpsc::Receiver<AgentEvent>>;
    async fn interrupt(&self) -> Result<()>;
    async fn cancel(&self) -> Result<()>;
    async fn dispose(&mut self) -> Result<()>;
}

// harness-core: Application Facade (CLI/TUI 使用)
#[async_trait]
pub trait HarnessApi: Send + Sync {
    async fn create_run(&self, objective: &str) -> Result<RunHandle>;
    async fn attach_run(&self, run_id: &str) -> Result<RunHandle>;
    // ... (完整接口见 cli-architecture.md)
}
```

---

## 5. 禁止的模式

```
❌ harness-core 引用 rusqlite / tokio / ratatui
❌ harness-runtime 引用具体 Adapter struct（只能引用 AgentAdapter trait）
❌ harness-adapters 引用 harness-runtime::persistence
❌ 任何 crate 通过字符串 "claude-code"/"codex" 做 if-else 分发
❌ 空壳 crate（Cargo.toml 存在但 src/ 无实质代码）
❌ 两个 crate 互相依赖（循环）
❌ 未使用的 trait 方法
❌ 公开接口没有调用方或 contract test
```

## 6. 为什么是 4 个 Crates（不是更多）

| 考量 | 决策 |
|------|------|
| 构建时间 | 4 crates 并行编译，无需过度拆分 |
| 依赖隔离 | core 零依赖保证可测试性 |
| 发布粒度 | 整个 Harness 作为单个二进制发布，无需独立 crate 版本 |
| 未来扩展 | 如需独立 adapter crate（如 `harness-adapter-gemini`），可以添加第 5 个 |
| 碎片化风险 | 超过 6-8 个 crate 前需要 ADR 批准 |
