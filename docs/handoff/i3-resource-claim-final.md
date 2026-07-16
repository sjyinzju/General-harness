# I3 Resource Claim Kernel — Final Closure Report

> **状态**: I3 完成，质量门全绿，就绪交付 I4
> **日期**: 2026-07-16
> **Branch**: `main`
> **HEAD**: `d0d134e` (feat(i3-c): integrate resource claims with lease and recovery)

---

## 1. 接手审计摘要

| 项目 | 状态 |
|---|---|
| 当前 HEAD | `adc05da` → I2B final handoff commit |
| 分支 | `main` |
| 工作树 | clean |
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace` | **286 passed / 0 failed / 0 ignored** |
| `git diff --check` | PASS |
| `git status --short` | empty |

**已有 ResourceClaim 基础设施:**

- `ResourceClaim` struct 存在于 `harness-core/src/contracts/task_envelope.rs`（Gate C frozen wire type）
- `resource_claims` 表存在于 migration 001（Gate C frozen），列：id, project_id, task_id, execution_id, resource_kind, normalized_resource, access_mode, status, heartbeat_at, expires_at, acquired_at, released_at
- 无现有 ResourceClaimRepository 或 Service
- `docs/architecture/resource-claim-model.md` 为设计文档，非实现代码
- 无 stub、TODO、ignored test 或绕过 frozen contract 的实现

**I2B Handoff 一致性**: 已验证。所有不变量（fencing fail-closed, token 不泄漏, fingerprint 完整性）保持 intact。

**审计结论**: 可以开始 I3。

---

## 2. 三个批次 Commit

| 批次 | Commit | 标题 |
|---|---|---|
| I3-A | `c05d621` | feat(i3-a): add resource claim model and conflict engine |
| I3-B | `10161f6` | feat(i3-b): add atomic resource claim persistence |
| I3-C | `d0d134e` | feat(i3-c): integrate resource claims with lease and recovery |

---

## 3. 最终质量门

| 命令 | 结果 |
|---|---|
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace` | PASS，**386 passed / 0 failed / 0 ignored** |
| `git diff --check` | PASS |
| `git status --short` | clean |

测试增长：286 (I2B) → 386 (I3)，+100 测试：
- I3-A: +52 纯逻辑测试（harness-core）
- I3-B: +22 持久化测试（resource_claim_persistence）
- I3-C: +19 集成测试（resource_claim_integration）+7 adapter 单测

---

## 4. Resource Model

### ResourceKind (4 种)

```rust
enum ResourceKind {
    ExactFile,        // 精确文件路径
    DirectoryPrefix,  // 目录前缀
    RepositoryWide,   // 整个仓库
    Logical,          // 逻辑资源
}
```

### AccessMode (2 种)

| | READ | WRITE |
|---|---|---|
| **READ** | ✅ 兼容 | ❌ 冲突 |
| **WRITE** | ❌ 冲突 | ❌ 冲突 |

### 路径重叠算法

使用组件语义（component-path）：
- `src/a/` 与 `src/a/b.rs`：重叠（directory prefix 包含 exact file）
- `src/a/` 与 `src/a/b/`：重叠（nested directories）
- `src/a/` 与 `src/ab/`：**不重叠**（component boundary，非 substring）
- 不同 repository identity：永不重叠
- 路径 vs Logical：永不重叠（不同 domain）

路径规范化：
- Unicode NFC normalization
- Windows case folding（lowercase）
- 分隔符规范化（`\` → `/`）
- 拒绝 `..` traversal、ADS（`:`）、Windows 保留设备名
- 拒绝首尾空格、超长组件

---

## 5. Claim Group 原子语义

- 多资源请求归一化（dedup、Read→Write 升级、目录覆盖精确文件）
- 全部兼容 → 全部获取
- 任一冲突 → 零获取（禁止部分成功）
- 稳定排序 + SHA-256 request hash
- 幂等性：相同 key + 相同 hash → 返回已有 group；相同 key + 不同 hash → IdempotencyConflict
- 数据库提交成功但调用方未收到响应 → 重试返回 AlreadyAcquired

---

## 6. SQLite 跨连接仲裁

- 所有 acquire/replace 操作使用 `BEGIN IMMEDIATE` 事务
- 事务内：加载 active claims → 运行 overlap engine → insert group + rows
- Conflict check 与 insert 在同一串行化区间
- 两个不同连接同时请求冲突资源 → 最多一个成功

---

## 7. Persistence (Migration 008)

新增 `resource_claim_groups` 表：
- group_id, project_id, task_id, execution_id, repository_identity
- worktree_id, lease_id, fencing_token, request_hash
- lifecycle (active/released/expired), heartbeat_at, expires_at
- acquired_at, released_at, release_reason, version

扩展 `resource_claims` 表：
- 新增 group_id (FK → resource_claim_groups)
- 新增 lifecycle, created_at

Migrations 001–007 未修改。业务表从 14 增加到 15。

---

## 8. DomainEvent

- `resource_claim_group_acquired`
- `resource_claim_group_released`
- `resource_claim_group_expired`
- `resource_claim_group_replaced`
- `resource_claim_conflict_observed`（采样，非高频）

状态变更与事件同事务。使用递增 stream_version 避免 UNIQUE 冲突。

---

## 9. Lease/Fencing Integration

`ResourceClaimService` 注入 `ResourceClaimLeaseValidator`：
- acquire/renew/replace/release 前验证 lease_id + lease_token + fencing_token
- 旧 fencing token 无法执行任何变更操作
- Claim expires_at 不得晚于 Lease expires_at
- lease_token 不存入 SQLite、不进入 Debug/Display/Event/tracing
- ClaimGuard 自定义 Debug 输出 [REDACTED]

---

## 10. Reconciler

`ResourceClaimReconciler` 检测 13 种异常类型：
- ACTIVE_BUT_EXPIRED → 自动 expire
- ACTIVE_LEASE_RELEASED / ACTIVE_LEASE_EXPIRED → 自动 expire
- STALE_FENCING_TOKEN → 报告但不自动修复
- OWNER_EXECUTION_TERMINAL / OWNER_EXECUTION_LOST → 自动 expire
- WORKTREE_MISSING → 报告
- WORKTREE_REMOVED → 自动 expire
- CLAIM_GROUP_WITHOUT_ROWS → 自动 expire
- CLAIM_ROWS_WITHOUT_GROUP → 报告
- MULTIPLE_CONFLICTING_ACTIVE_GROUPS → 报告
- 不自动恢复 terminal group
- 不删除 Worktree
- 不抢占合法 owner

---

## 11. TaskEnvelope Adapter

`derive_claims_from_envelope()` 将 frozen TaskEnvelope 转换为保守 ClaimGroupSpec：
- exact write scope → ExactFile Write
- directory write scope → DirectoryPrefix Write
- exact read scope → ExactFile Read
- directory read scope → DirectoryPrefix Read
- glob 提取静态目录前缀 → DirectoryPrefix
- 无法安全收窄的 write glob → RequiresExplicitClaim
- forbidden scope → 不生成 Claim
- 显式 `resource_claims` → 直接映射
- Gate C frozen TaskEnvelope 未修改

---

## 12. 测试覆盖

| 测试文件 | 测试数 | 覆盖范围 |
|---|---|---|
| harness-core (resource_claim) | 52 | 模型、规范化、冲突矩阵、overlap engine |
| resource_claim_persistence | 22 | 原子获取、idempotency、跨连接并发、replace、expiry |
| resource_claim_integration | 19 | Lease/fencing 验证、reconciler、TTL、guard |
| harness-runtime (adapter tests) | 7 | TaskEnvelope 推导、glob、logical |
| **I3 新增合计** | **100** | |
| **总数 (I2B+I3)** | **386** | |

---

## 13. 平台限制

- SQLite 单写者（WAL mode, busy_timeout=5s）
- Windows 路径大小写不敏感在 normalize 中处理（lowercase）
- Windows Job Object 相关功能未改动
- Unicode 规范化使用 `unicode-normalization` crate

---

## 14. 未完成项（明确排除）

- Task DAG Scheduler（I4）
- Agent 分配 / 并发 slot（I4）
- 生产 Claude/Codex Adapter（I4）
- Verification Pipeline（I4）
- Task retry loop（I4）
- Commit/Integration Queue（I4）
- Supervisor IPC（I4）
- TUI（I4）
- Project Goal Loop（I4）
- 等待队列与公平性（I4 Scheduler）
- 任务优先级 / 抢占（I4+）
- 分布式数据库（后续）
- 跨 repository logical resource（后续）
- 动态 LLM 重划 scope（后续）
- 跨项目共享资源（后续）

---

## 15. I3 退出条件核验

| 条件 | 满足 |
|---|---|
| 四种 ResourceKind 完成 | ✅ ExactFile, DirectoryPrefix, RepositoryWide, Logical |
| READ/WRITE 冲突矩阵 | ✅ Read+Read 兼容，其余冲突 |
| 路径 overlap 使用组件语义 | ✅ `src/a` ≠ `src/ab` |
| Claim Group 全有或全无 | ✅ 任一冲突 = 零插入 |
| 跨连接并发只有合法 winner | ✅ BEGIN IMMEDIATE 串行化 |
| 相同幂等请求不产生重复 group | ✅ AlreadyAcquired |
| 状态与 DomainEvent 同事务 | ✅ |
| 生产服务强制 Lease/Fencing 验证 | ✅ ResourceClaimService |
| Claim 不得超过 Lease 生命周期 | ✅ bound_duration |
| 旧 owner 无法 acquire/renew/replace/release | ✅ FencingRejector/TokenRejector |
| Reconciler 完成 | ✅ 13 种 anomaly |
| 冲突只返回诊断 | ✅ ClaimDecision::Conflict |
| migrations 001–007 未修改 | ✅ 仅新增 008 |
| 全部测试 0 failed, 0 ignored | ✅ 386/0/0 |
| 无 Gate C frozen contract blocker | ✅ TaskEnvelope 未修改 |

---

## 16. 是否进入 I4

**可以。** I3 退出条件全部满足。I4 Scheduler 可利用 ResourceClaimService 进行确定性并发控制。

---

**就绪交付 I4。**
