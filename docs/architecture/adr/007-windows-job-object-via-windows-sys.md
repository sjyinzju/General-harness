# ADR-007: Windows Job Object via windows-sys FFI

- **Status**: Accepted
- **Date**: 2026-07-15
- **Phase**: I2B-0 Process/Artifact Carryover

## Context

I2A 交接文档（`docs/handoff/i2a-process-kernel-handoff.md` §5）声称：

> `windows` crate 0.58 `.map_err()` return type differs from current stable
> Rust patterns — `CreateJobObjectW` returns `Result<HANDLE, Error>` but the
> error mapping via `.code().0` is unstable

并以此将 Windows Job Object 推迟到 I2B-0，Foundation 临时以
`taskkill /PID <pid> /T /F` 作为进程树终止的主路径。

`taskkill /T` 的已知缺陷：它按父子 PID 链遍历进程树。当中间进程已退出时，
孤儿化的孙进程无法被发现，会在 Execution 结束后残留。

## 复现结果（2026-07-15, rustc 1.95.0）

在独立临时工程（`%TEMP%\winjob-repro-058`）中，以 `windows = "0.58"` +
features `Win32_Foundation / Win32_System_JobObjects / Win32_Security /
Win32_System_Threading` 复现交接文档所述的完整模式：

```rust
let job = CreateJobObjectW(None, None).map_err(|e| format!("code={}", e.code().0))?;
SetInformationJobObject(job, JobObjectExtendedLimitInformation, ..)
    .map_err(|e| format!("set: code={}", e.code().0))?;
AssignProcessToJobObject(job, GetCurrentProcess()).map_err(..)?;
TerminateJobObject(job, 0).map_err(..)?;
CloseHandle(job);
```

**结论：编译并运行成功（exit 0）。交接文档声称的编译不兼容不可复现。**
唯一的编译坑是 windows 0.58 泛型参数 `Param<HANDLE>` 在"引用函数而不调用"
时无法推断类型（E0283）——这不影响正常调用。

## Decision

采用 **`windows-sys` 0.61**（而非 `windows` 0.58）实现 Job Object：

1. **零新增依赖**：`windows-sys` 0.61.2 已通过 tokio/sqlx 存在于依赖树，
   只需在 `harness-runtime` 声明为 `[target.'cfg(windows)'.dependencies]`
   直接依赖并启用 3 个 feature（`Win32_Foundation`、`Win32_Security`、
   `Win32_System_JobObjects`）。lock file 无任何版本变更。
2. **无泛型魔法**：`windows-sys` 是纯 `extern "system"` 声明（返回裸
   HANDLE/BOOL），不引入 `windows-core` 的 `Param`/`Result` 抽象层，
   FFI 语义与 Win32 文档一一对应，长期更稳。
3. **隔离的 unsafe 模块**：全部 unsafe 集中在
   `harness-runtime/src/process/job_object.rs` 的 `windows_job` 私有模块，
   每处 unsafe 附 SAFETY 注释。

## Implementation

- `JobObject::create_kill_on_close()` — `CreateJobObjectW` +
  `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`。
- `JobObject::assign_raw_handle(RawHandle)` — 接收 tokio
  `Child::raw_handle()`（tokio 1.52.3 提供）。
- `JobObject::terminate(exit_code)` — `TerminateJobObject`。
- `Drop` — `CloseHandle`；因 KILL_ON_JOB_CLOSE，句柄关闭即终止 job 内
  全部存活进程 → Supervisor 崩溃也不泄漏后代。
- `ProcessTreeGuard`（跨平台封装）：Windows 上 Job Object 为主路径，
  创建/assign 失败时降级 `taskkill /T /F` 并 `tracing::warn`；Unix 仍为
  进程组 kill。ProcessManager 在 spawn 后立即 attach，并在
  cancel/timeout/自然退出三条路径上统一 `kill_tree()`。

已知边界：assign 发生在 `spawn()` 返回之后，子进程在 assign 前抢先创建
的后代理论上可能短暂逃逸 job（窗口为毫秒级）。彻底消除需
CREATE_SUSPENDED 启动，Foundation 阶段接受该窗口。

## Verification

- `grandchild_tree_terminated`：root 存活期间 cancel，孤儿化孙进程
  （父已退出，taskkill /T 不可达）被终止 — PASS。
- `no_residual_descendants`：root 自然退出后，残留后代被 job 终止 —
  PASS。
- 全套 `cargo test --workspace`、`cargo clippy --workspace --all-targets
  -- -D warnings`、`cargo fmt --all --check` 通过。

## Consequences

- taskkill 从主路径降级为 fallback（仅 job 创建/assign 失败时）。
- I2A 交接文档 §5 的"windows crate 编译不兼容"记录不再成立，以本 ADR
  为准。
- 后续如需 CPU/内存限额，可在同一 Job Object 上扩展
  `JOBOBJECT_EXTENDED_LIMIT_INFORMATION`（无新依赖）。
