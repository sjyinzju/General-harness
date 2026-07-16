# I2B Workspace Kernel — Final Closure Report

> **状态**: I2B 三批次修复完成，质量门全绿，就绪交付独立复审
> **日期**: 2026-07-16
> **Branch**: `main`
> **HEAD**: `136f7eb`
> **复审方**: 新的 DeepSeek V4 Pro 会话（独立复审，本报告不自宣布审计通过）

本报告由执行修复的 GLM-5.2 会话撰写，基于独立审计报告（`docs/handoff/i2b-workspace-kernel-handoff.md` 之后的审计）发现的 Critical/High/Medium 问题逐项修复。修复分三个独立批次，每批次独立运行质量门并独立提交。

---

## 1. 三个 Commit

| 批次 | Commit | 标题 |
|---|---|---|
| Batch A | `031aa4f` | fix(i2b-3): close command policy and fencing bypasses |
| Batch B | `cda93f2` | fix(i2b-3): harden secret scanning and workspace paths |
| Batch C | `136f7eb` | feat(i2b-3): complete diff validation and policy reconciliation |

修复前 HEAD 为 `0cb68a9`（审计交接点）。三个 commit 叠加在其上。

---

## 2. 最终质量门

| 命令 | 结果 |
|---|---|
| `cargo fmt --all --check` | PASS（exit 0） |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS（exit 0，0 warning） |
| `cargo test --workspace` | PASS，**286 passed / 0 failed / 0 ignored** |
| `git diff --check` | PASS（exit 0，无空白错误） |
| `git status --short` | 空（工作树 clean） |

测试总数从审计时的 253 增长到 286（+33）：
- workspace_policy: 42 → 54（+12）
- workspace_lease: 57 → 58（+1）
- runtime unit（scanner/file_scope 等）: 47 → 56（+9）
- workspace_diff（新增端到端）: 0 → 9（+9）
- 其余不变。净 +33，其中 +2 为重命名/调整。

---

## 3. 审计问题修复状态

### Critical

| 编号 | 问题 | 状态 | 修复位置 |
|---|---|---|---|
| C1 | CommandPolicy 幂等仅用 args_hash，跨 executable/cwd/env 碰撞 | **已修复** | `command.rs` 新增 `CommandFingerprint::composite_key()`；`service.rs` 查询与存储改用 composite key。测试：`same_args_different_executable_no_cache_collision`、`same_args_different_cwd_no_cache_collision` |
| C2 | WorkspacePolicyService 不校验当前 lease/fencing，旧 token 可生成证据 | **已修复** | `service.rs` 注入 `LeaseFencingValidator`，`evaluate_command`/`persist_scan_evidence`/`validate_workspace_diff`/`record_approval` 入口 `enforce_fencing`；`WorkspaceLeaseService` 实现该 trait。测试：`stale_fencing_cannot_create_command_evidence`、`stale_fencing_cannot_create_scan_evidence`、`validate_workspace_diff_persists_evidence_and_fencing` |
| C3 | `git config` 等可写子命令被列为只读 Allow；`--global` 可致 RCE | **已修复** | `command.rs` 从 `allowed_git_read_only` 移除 config/branch/tag/remote/worktree/stash/notes；新增 `git_config_global` Deny 模式。测试：`git_config_global_denied`、`git_branch_delete_requires_approval_or_denied`、`git_worktree_add_not_read_only` |
| C4 | 构建工具（python/node/npx/go/cargo/pip）任意代码执行入口被无条件 Allow | **已修复** | `command.rs` 新增 `code_exec` 危险模式（python -c/-m、node -e/--eval、npx、pnpm dlx、go run、cargo run），在 build-tool 放行之前匹配。测试：`python_c_requires_approval`、`node_e_requires_approval`、`npx_requires_approval` |

### High

| 编号 | 问题 | 状态 | 修复位置 |
|---|---|---|---|
| H1 | 多 arg 危险模式（reset --hard / clean -fdx / rm -rf）永不命中 | **已修复** | `command.rs` `arg_contains` 改为对 `args.join(" ").to_lowercase()` 匹配；rm 覆盖 -rf/-fr/-r -f/-f -r。测试：`git_reset_hard_detected`、`git_clean_fdx_detected`、`recursive_delete_denied` |
| H2 | SecretFinding preview 泄漏原始 secret（私钥正文/token 主体/credential 值/相邻 secret） | **已修复** | `scanner.rs` 所有 preview 改为固定占位符，新增 `fingerprint` 哈希字段与 line/byte 范围。测试：`no_raw_secret_in_any_finding_kind`、`finding_contains_no_raw_secret`、scanner 单测逐类断言 |
| H3 | 多字节 UTF-8 文本被误判 binary 并跳过扫描 | **已修复** | `scanner.rs` `is_binary` 改为：有效 UTF-8 视为文本（仅 NUL 比例 >0.10 才 binary）；非 UTF-8 按 NUL/控制字节比例判定。测试：`utf8_cjk_text_not_binary`、`utf8_cjk_secret_scanned` |
| H4 | FileScope 大小写/Unicode 不敏感文件系统上 fail-open（`.GIT`、`Secret/`） | **已修复** | `file_scope.rs` `normalize()` 做 NFC + Windows lowercase；匹配统一用规范化串。测试：`gitmeta_case_insensitive`、`forbidden_case_insensitive`、`unicode_normalization` |
| H5 | FileScope 不拒绝 Windows ADS（`:`） | **已修复** | `file_scope.rs` 组件含 `:` → `AlternateDataStream`（Windows）。设备名尾空格/点处理。测试：`ads_rejected`、`reserved_trailing_space` |

### Medium

| 编号 | 问题 | 状态 | 修复位置 |
|---|---|---|---|
| M1 | `ServiceLeaseAccessValidator` 把 worktree_id 当 lease_id 传给 validate_lease，admin recovery 路径失效 | **已修复** | `access_validator.rs` 改传 `cred.lease_id`；`validate_force_credential` 增加归属 worktree 校验。测试：`valid_admin_recovery_credential_accepted` |
| M2 | `validate_approval` 过期为字符串比较无时区 | **部分修复** | 返回类型改为 `ApprovalOutcome` 枚举；仍用字符串字典序比较零填充时间戳（UTC）。审批持久化已绑定 fencing epoch。完整时区语义未做（见 §5 未修复项） |
| M3 | `executable_contains` 子串过宽/过窄 | **部分修复** | sh/cmd/powershell 子串匹配保留（deny 方向安全）；cmd 单独添加；pwsh 由 sh 覆盖。未做精确化（见 §5） |
| M4 | 单前导反斜杠 `\foo` 分类不准 | **已修复** | `file_scope.rs` 绝对路径检查改用分隔符规范化形式 `n`，统一捕获 `/etc`、`C:/`、`\foo`、UNC |
| M5 | 高熵检测近乎无效（全文含空格即跳过） | **已修复** | `scanner.rs` 改为逐行检测，带 line/byte 范围 |
| M6 | 截断边界漏扫且可能静默 clean | **已修复** | 截断产生 `TruncatedLargeFile` finding → `clean=false`；测试 `truncated_file_not_clean` |
| M7 | 删除内容不扫描 | **未修复（设计决定）** | 见 §5 |

---

## 4. 三项声明性交付

### Git Diff Scope Validator（Batch C，已实现）
- 模块：`crates/harness-runtime/src/policy/diff.rs`
- 所有 git 调用经 `GitRunner`/`ProcessManager`，机器可读输出（`-z`），成功仅看 exit code，不依赖本地化 stderr。
- 覆盖：staged（`--cached`）、unstaged、untracked（`ls-files --others --exclude-standard`）。
- 变更类型：Added / Modified / Deleted / Renamed / Copied / TypeChange / Binary（`numstat -z` 的 `-`）/ Submodule（`ls-files -s` mode 160000）/ Untracked。
- rename/copy 同时校验 source 与 destination，记录 `RenameEvidence`。
- 真实 changed paths 来自 git，不依赖 Agent 自报。
- 生成 `ScopeValidationReport` + Policy Evidence（evaluation_type=`diff`）；大型 diff 用 `artifact_reference`，完整 diff 不入 SQLite。
- 端到端测试（真实临时 git repo）：`diff_detects_staged_unstaged_untracked`、`diff_flags_out_of_scope`、`diff_rename_validates_both_sides`、`diff_binary_detected`。

### Policy Reconciler（Batch C，已实现）
- 模块：`crates/harness-runtime/src/policy/reconciler.rs`
- 交叉检查：evidence ↔ worktree 存在性 ↔ 当前 fencing epoch ↔ policy version ↔ artifact reference 存在性。
- 旧 fencing、旧 policy version、Worktree 缺失、artifact 丢失 → 标记 `invalid`（sentinel decision），不再作为后续 Commit/Verification 有效依据。
- `WorkspacePolicyService.evaluate_command` 幂等路径通过 `is_reusable_decision` 跳过 invalid 行。
- 测试：`reconciler_marks_stale_fencing_and_invalidates`、`reconciler_marks_lost_artifact`、`invalidated_evidence_not_reused_for_idempotency`。

### Approval Contract（Batch C，持久化边界已补齐，UI 未实现）
- Migration `007_policy_approvals.sql`：`policy_approvals` 表，键为 composite command fingerprint + fencing epoch。
- `PolicyEvidenceStore::insert_approval` / `find_approval`；`WorkspacePolicyService::record_approval` / `find_approval` 均 fencing-gated（旧 epoch 的审批不可复用）。
- `validate_approval` 返回 `ApprovalOutcome { Approved, Expired, FingerprintMismatch }`。
- 交互式审批 UI 未实现（明确非目标）。
- 测试：`approval_persistence_roundtrip`。

---

## 5. 未修复项及理由

| 项 | 理由 |
|---|---|
| M2 审批过期完整时区语义 | `validate_approval` 用零填充 UTC 时间戳字典序比较，对单一 UTC 源正确；跨时区场景需审批系统层面的时区策略，属 Approval UI 范畴（明确非目标）。已记录待 UI 阶段处理 |
| M3 executable_contains 精确化 | sh/cmd/powershell 子串匹配在 deny 方向是 fail-closed（误杀安全），且 pwsh 由 sh 覆盖。当前无真实绕过；精确化（如 exact set）留作后续策略调优，不阻塞 I2B |
| M7 删除内容不扫描 | 扫描器仅扫新增内容是既有设计（`deleted_secret_does_not_block` 固化）。删除泄漏的语义需 diff 语义层面的产品决策，不属 I2B-3 安全硬阻塞 |
| Approval UI | 任务明确排除 |
| 完整 ResourceClaim 冲突算法 / Scheduler / Loop Engine / Supervisor IPC / TUI | I3 及之后阶段，I2B 禁止 |

---

## 6. 不变量（修复后）

- **Fencing fail-closed**：所有 Evidence 生成入口（command / scan / diff / approval）均先 `enforce_fencing`；旧/无效 lease 不产生证据。`WorkspaceLeaseService` 实现生产 `LeaseFencingValidator`。
- **Token 不泄漏**：`lease_token` 仅存于 `WorkspaceAccessGuard`（自定义 Debug 脱敏）与内存 `LeaseRecord`（自定义 Debug 脱敏），不入数据库、不入 event payload、不入 PolicyEvidence。证据仅存 fencing epoch。
- **Secret 不泄漏**：所有 `SecretFinding.redacted_preview` 为固定占位符；secret 以非可逆 `fingerprint` 哈希 + line/byte 范围表示。
- **Fingerprint 完整性**：幂等查询/存储使用 `CommandFingerprint::composite_key()`（exec+args+cwd+env_names），无跨维度碰撞。
- **生产构造器 fail-closed**：`WorkspaceLeaseService::new` 强制 `WorktreeGitVerifier`；`WorktreeManager::new` 强制 `WorkspaceLeaseAccessValidator`；`WorkspacePolicyService::new` 强制 `LeaseFencingValidator`。测试专用 `new_unverified_for_tests` / `new_unleased` 命名明确，仅测试使用。
- **Gate C 冻结**：migrations 001–003 未触碰；004–006 未改动；新增 007（policy_approvals）为 additive。业务表 14 张（001–007）。

---

## 7. I2B 退出条件核验

| 条件 | 满足 |
|---|---|
| Process/Artifact carryover (I2B-0) | 是 |
| WorktreeManager v1 (I2B-1) | 是 |
| WorkspaceLeaseService v1 (I2B-2) | 是 |
| Workspace Policy v1 (I2B-3) | 是（含 Diff Validator + Reconciler） |
| fmt / clippy(-D warnings) / test 全绿 | 是（286/0/0） |
| 工作树 clean / `git diff --check` | 是 |
| Gate C 冻结合约未触碰 | 是 |
| `lease_token` 不泄漏 logs/display/events | 是 |
| `SecretFinding` 不含完整 secret | 是（占位符 + 哈希） |
| 生产构造器 fail-closed | 是（lease verifier / worktree validator / policy fencing validator 均强制） |
| 旧 fencing token 无法产生有效 Evidence | 是（enforce_fencing + reconciler 标记 invalid + 幂等跳过 invalid） |
| Commit 历史干净 | 是（3 个修复 commit） |

**执行方结论**：I2B 退出条件在修复后满足。**但本结论需由新的 DeepSeek V4 Pro 会话独立复审确认**，本会话不自行宣布审计通过。

---

## 8. 是否进入 I3

**不进入。** 按指令，I2B 修复结果交付独立复审，复审通过前不开始 I3 Resource Claim。

---

**就绪交付独立复审。**
