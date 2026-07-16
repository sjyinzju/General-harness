# I2B Workspace Kernel Handoff

> **状态**: I2B Workspace Kernel Complete — 就绪进行独立审计
> **日期**: 2026-07-16
> **Branch**: `main`

---

## 1. Commit 历史

| Phase | Commit | 描述 |
|-------|--------|------|
| I2B-0 | `dc9d08f` | feat(i2b-0): add artifact capture and process hardening |
| I2B-1 | `a71b30a` | feat(i2b-1): add worktree manager with saga operations and reconciliation |
| I2B-2 | `34b88c4` | feat(i2b-2): add workspace lease service with fencing and reconciliation |
| I2B-3 | `f2c742f` | feat(i2b-3): add workspace scope and command policy enforcement |

HEAD: `f2c742f`

---

## 2. 模块源码路径

### I2B-0: Process/Artifact Carryover
```
crates/harness-runtime/src/artifact.rs              — RuntimeArtifactDirectory
crates/harness-runtime/src/process/capture.rs       — Stream capture (bounded channels, spool)
crates/harness-runtime/src/process/redactor.rs      — ProcessEventRedactor
crates/harness-runtime/src/process/job_object.rs    — Windows Job Object (KILL_ON_JOB_CLOSE)
crates/harness-runtime/src/process/manager.rs       — ProcessManager (spawn/drain/timeout/cancel)
crates/harness-runtime/src/process/types.rs         — ProcessSpec/Outcome/CapturePolicy
crates/harness-runtime/tests/process_capture.rs     — 14 项洪水/UTF-8/limit/spool/race 测试
```

### I2B-1: WorktreeManager
```
crates/harness-runtime/src/worktree/git.rs          — GitRunner (git CLI via ProcessManager)
crates/harness-runtime/src/worktree/inspector.rs    — RepositoryInspector
crates/harness-runtime/src/worktree/manager.rs      — WorktreeManager (create/inspect/remove Saga)
crates/harness-runtime/src/worktree/reconciler.rs   — WorktreeReconciler (12 类 drift)
crates/harness-runtime/src/worktree/git_verifier.rs — WorktreeGitVerifier trait
crates/harness-runtime/src/worktree/lock.rs          — RepositoryLocks (async per-repo)
crates/harness-runtime/src/worktree/metadata.rs     — Sidecar ownership (.harness.json)
crates/harness-runtime/src/worktree/naming.rs        — 路径/branch 净化 + canonicalize_for_git
crates/harness-runtime/src/worktree/types.rs         — WorktreeSpec/Record/Inspection
crates/harness-runtime/migrations/004_worktrees.sql  — worktrees 表
crates/harness-runtime/tests/worktree_manager.rs     — 30 项测试
```

### I2B-2: WorkspaceLeaseService
```
crates/harness-runtime/src/lease/service.rs          — WorkspaceLeaseService
crates/harness-runtime/src/lease/transition.rs       — LeaseTransitionService
crates/harness-runtime/src/lease/guard.rs            — WorkspaceLeaseAccessValidator trait
crates/harness-runtime/src/lease/access_validator.rs — ServiceLeaseAccessValidator
crates/harness-runtime/src/lease/reconciler.rs       — WorkspaceLeaseReconciler (12 类 drift)
crates/harness-runtime/src/lease/runner.rs           — LeaseHeartbeatRunner
crates/harness-runtime/src/lease/clock.rs            — Clock trait + SystemClock + TestClock
crates/harness-runtime/src/lease/types.rs            — LeaseSpec/Record/AcquireOutcome
crates/harness-runtime/migrations/005_workspace_lease_v2.sql — workspace_leases 扩展
crates/harness-runtime/tests/workspace_lease.rs      — 57 项测试
```

### I2B-3: Workspace Policy
```
crates/harness-runtime/src/policy/file_scope.rs     — FileScopeValidator
crates/harness-runtime/src/policy/command.rs        — CommandPolicyEngine + Approval
crates/harness-runtime/src/policy/scanner.rs        — SecretScanner
crates/harness-runtime/src/policy/evidence.rs        — PolicyEvidenceStore
crates/harness-runtime/src/policy/service.rs         — WorkspacePolicyService
crates/harness-runtime/migrations/006_policy_evidence.sql — policy_evaluations + policy_findings
crates/harness-runtime/tests/workspace_policy.rs     — 42 项测试
```

---

## 3. 当前质量门

```
cargo fmt --all --check        PASS (0)
cargo clippy --workspace       PASS (0 errors, -D warnings)
cargo test --workspace         253 passed / 0 failed / 0 ignored
git status --short             (clean)
```

---

## 4. I2B 已完成的不变量

### Process
- ProcessOutcome 每条进程只产生一次（RwLock guard）
- stdout/stderr 独立 reader + 有界 channel → 洪水输出永不卡死 harness
- Spool 文件写入 `RuntimeArtifactDirectory`，不在用户 git worktree 内
- Windows Job Object (KILL_ON_JOB_CLOSE) → supervisor 崩溃不泄漏后代进程
- CapturePolicy::Pipe 的 byte_limit 为硬上限（超出后记数不存储）

### Worktree
- 所有 worktree 管理操作经过 Operation/Saga → 幂等
- 路径和 branch 净化（防 `..`、绝对路径、Windows 设备名、`check-ref-format`）
- Sidecar ownership metadata 在 worktree 外，不污染 git diff
- `WorktreeManager::new()` 强制 `Box<dyn WorkspaceLeaseAccessValidator>`
- Dirty/unknown worktree 不会被动删除

### Lease
- Monotonic fencing token（`worktrees.lease_epoch` 原子递增）
- 状态变更 + DomainEvent 同 SQLite 事务
- Terminal lease 不可恢复；重新获取 = 新 lease_id + 更高 fencing
- 旧 owner 无法 heartbeat/release/validate 接管后的 lease
- `lease_token` 不出现在 Debug/Display/tracing/event payload
- 生产 `WorkspaceLeaseService::new()` 强制 `WorktreeGitVerifier`

### Policy
- CommandPolicyEngine: executable + arg AND 逻辑（非独立触发）
- SecretScanner: binary detection 正确排除 space（0x20）
- SecretFinding 不保存完整 secret；PolicyEvidence 不含 lease token
- 生产 Lease Acquire 不可跳过 Git verifier

---

## 5. Workspace Policy 已实现内容

- **FileScopeValidator**: 逐组件检查、glob 匹配、前缀混淆检测、symlink escape、git/harness metadata 保护、Windows 设备名拒绝、partial canonicalize（不存在路径走最近祖先）
- **CommandPolicyEngine**: exec+args+cwd+env_names 四要素 AND 逻辑；默认允许（build tools、read-only git）、拒绝（shell、rm -rf、setx、curl|sh）、审批（git push、--force、包管理全局）
- **CommandFingerprint**: 稳定哈希绑定 exec+args+cwd+env_names
- **ApprovalRequest/Decision**: 指纹匹配 + 过期验证
- **SecretScanner**: 已知 secret 精确匹配、私钥头、API token pattern、高熵检测、credential 文件路径、binary 跳过、大文件截断、扫描总量限制
- **PolicyEvidenceStore**: policy_evaluations + policy_findings 持久化、fingerprint 幂等、stale 证据失效化
- **WorkspacePolicyService**: evaluate_command、scan_diff、persist_scan_evidence、invalidate_stale_evidence、validate_approval

---

## 6. 未完成内容

### Git Diff Scope Validator（未实现）
I2B-3 spec §4 要求的 `ScopeValidationReport`（changed_paths、allowed_changes、violations、rename_evidence、untracked_evidence）未实现。需要：
- 通过 GitRunner 执行 `git diff --name-status -z` 解析
- 将结果与 FileScopeValidator 集成
- 生成 PolicyEvidence

### Policy Reconciliation（未实现）
I2B-3 spec §10 要求的 Policy Evidence 与 worktree/lease/fencing 交叉检查未实现。需要：
- 确认 Evidence 对应 Worktree 未被替换
- 检测旧 fencing token
- 标记 stale/invalid

### Approval Contract（结构已定义，持久化/UI 未实现）
- `ApprovalRequest`、`CommandFingerprint` 结构定义完成
- `WorkspacePolicyService::validate_approval()` 完成
- Approval 持久化和交互式审批 UI 未实现

### Git Worktree 身份验证（接口已定义，Lease 集成未完成）
- `WorktreeGitVerifier` trait + `RepositoryInspector` 实现完成
- `WorkspaceLeaseService` 注入路径完成
- 测试中未通过真实 git repo 端到端验证

---

## 7. FileScopeValidator 完整重写

**涉及文件**: `crates/harness-runtime/src/policy/file_scope.rs`

**原因**: 在 I2B-3 Closure 阶段修复 `prefix_confusion_detected` 测试时，PowerShell regex 替换 (`-replace`) 错误地删除了文件中所有 `));` 模式，导致 Rust 语法损坏无法增量修复。最终以完整重写恢复。

**功能无退化**: 重写版本保留了所有原有 validator 功能（path escape、glob 匹配、metadata 保护、设备名拒绝、partial canonicalize），并修正了 prefix confusion 逻辑。

---

## 8. 已知 Bug 与修复

### CommandPolicyEngine AND/OR bug（I2B-3 Closure 修复）
- **文件**: `crates/harness-runtime/src/policy/command.rs`
- **根因**: `DangerousPattern` 的 `executable_contains` 和 `arg_contains` 被实现为两个独立 `if` 块。若 `executable_contains` 匹配即立即返回，**忽略 `arg_contains` 约束**。导致 `git_push` 模式匹配了所有 `git *` 命令（包括 `git status`）。
- **修复**: 重构为 AND 逻辑：`exec_match && arg_match`。无约束条件（`None`）视为 `true`。
- **影响范围**: 所有 CommandPolicy 判断均受影响。

### SecretScanner binary detection bug（I2B-3 Closure 修复）
- **文件**: `crates/harness-runtime/src/policy/scanner.rs`
- **根因**: Binary detection 过滤器排除 `\n`、`\r`、`\t`，但遗漏 `' '`（space, 0x20）。`is_ascii_graphic()` 的范围是 `0x21..=0x7E`，不包含 space。**所有文本文件被误判为 binary**，导致 SecretScan 静默跳过全部扫描。
- **修复**: 在例外列表中添加 `**b != b' '`。
- **影响范围**: 所有文本 diff 的 SecretScan 均受影响。

---

## 9. Migration

| # | 文件 | 内容 |
|---|------|------|
| 001 | `001_initial_schema.sql` | 10 张业务表（Gate C frozen） |
| 002 | `002_idempotency_ownership.sql` | Idempotency ownership 模型 |
| 003 | `003_operation_claim.sql` | Operation claim 列 |
| 004 | `004_worktrees.sql` | worktrees 表 + partial unique indexes |
| 005 | `005_workspace_lease_v2.sql` | workspace_leases 扩展 + lease_epoch + 4 partial unique indexes |
| 006 | `006_policy_evidence.sql` | policy_evaluations + policy_findings 表 |

001–003 为 Gate C frozen；004–006 为 I2B 阶段增量。

---

## 10. I2B 退出条件

以下条件全部满足，I2B Workspace Kernel 可正式关闭：

- [x] Process/Artifact carryover (I2B-0) 完成
- [x] WorktreeManager v1 (I2B-1) 完成
- [x] WorkspaceLeaseService v1 (I2B-2) 完成
- [x] Workspace Policy v1 (I2B-3) 完成
- [x] 253 tests / 0 failed / 0 ignored
- [x] clippy -D warnings clean
- [x] fmt clean
- [x] Working tree clean
- [x] Gate C frozen contracts 未被触碰
- [x] `lease_token` 不泄漏到 logs/display/events
- [x] `SecretFinding` 不含完整 secret
- [x] 生产构造器 fail-closed（lease git verifier 强制，worktree lease validator 强制）
- [x] Commit 历史干净（4 个 feature commits）

未完成（I3 或后续阶段）：
- Git Diff Scope Validator
- Policy Reconciliation
- Approval 持久化/UI
- 完整 ResourceClaim 冲突算法
- Scheduler、Loop Engine、Supervisor IPC、TUI

---

## 11. 前置条件

**I3 Resource Claim 不得在 I2B Closure 前开始。**

I2B-3 主体代码 (`f2c742f`) 已提交，质量门全绿。Git Diff Scope Validator 和 Policy Reconciliation 是 I2B-3 spec 中的声明性未完成项，应在 I3 启动前由审计确认是否需要作为 I2B Closure 的硬阻塞项。

---

**就绪进行独立审计。**
