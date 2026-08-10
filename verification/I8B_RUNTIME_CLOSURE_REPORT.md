# I8B Runtime Closure — Final Report

> Real-TTY Supervisor bootstrap closure. Diagnoses the 15s health-timeout
> failure, fixes the root cause, and adds regression tests.

## 1. Problem Statement

`harness-cli` (no-arg, on a real TTY) compiled successfully but the TUI never
appeared. The auto-spawned Supervisor child exited within ~0.2s, yet
`ensure_supervisor` waited the full 15s before reporting a generic timeout:

```
Supervisor not reachable — starting it (harness supervisor start)...
Supervisor started (PID: 9788) for state directory: default
Error: SupervisorBootstrap("supervisor did not become healthy within 15s")
```

## 2. Root Cause

**Primary**: The `supervisor run` child crashed during
`bootstrap_production_graph` because the default worktree root
(`repo_root/target/tmp`) is inside the git repository.
`WorktreeManager::new()` rejects paths inside a git worktree, causing the
child to call `exit(1)` before the IPC server started.

**Secondary (diagnostic gap)**: `cmd_supervisor_start` spawned the child
with `DETACHED_PROCESS` and did not pipe stderr. The child's crash message
was lost. `ensure_supervisor` never checked whether the child was still
alive, causing a blind 15s wait.

**Identity**: All identity inputs (Named Pipe `harness-supervisor`, state
directory `default`, DB path, binary path) were consistent between the TUI
client and the Supervisor child. The failure was NOT an identity mismatch.

Full diagnosis: [docs/I8B_RUNTIME_CLOSURE_DIAGNOSIS.md](../docs/I8B_RUNTIME_CLOSURE_DIAGNOSIS.md)

## 3. Fixes

Three minimal, production-grade changes. No timeouts were increased. No
domain semantics were modified.

### Fix 1: Return Child handle + pipe stderr (`supervisor.rs`)

**File**: `crates/harness-cli/src/commands/supervisor.rs`

- Changed `cmd_supervisor_start` return type from
  `Result<(), Box<dyn std::error::Error>>` to
  `Result<std::process::Child, Box<dyn std::error::Error>>`.
- Added `cmd.stdout(Stdio::null())` and `cmd.stderr(Stdio::piped())` so
  callers can read crash diagnostics.
- Dropping the `Child` does NOT kill the process (documented).

### Fix 2: Update callers (`main.rs`)

**File**: `crates/harness-cli/src/main.rs`

- Two call sites updated: `Ok(())` → `Ok(_child)` to accept the new
  return type without holding the handle (fire-and-forget start).

### Fix 3: Safe worktree root + early-exit detection (`tui/mod.rs`)

**File**: `crates/harness-cli/src/tui/mod.rs`

Three new functions and a rewritten `ensure_supervisor`:

1. **`safe_worktree_root(repo_root)`** — returns `repo_root/target/tmp`
   when it is NOT inside a git worktree; otherwise falls back to
   `%LOCALAPPDATA%\harness\worktrees` (Windows) or
   `$XDG_DATA_HOME/harness/worktrees` (Unix). Always returns a path
   outside any git worktree.

2. **`path_inside_git_worktree(path)`** — walks the ancestor chain
   looking for `.git`, stopping at the user's home directory. Mirrors
   `harness_runtime::artifact::find_git_ancestor`.

3. **`read_child_stderr(child, max_bytes)`** — reads up to `max_bytes`
   from the child's piped stderr, returning the tail. Called only after
   the child has exited (via `try_wait`), so the read is non-blocking.

4. **`ensure_supervisor` rewrite** — now:
   - Computes a safe worktree root and passes it to
     `cmd_supervisor_start`.
   - Receives the `Child` handle.
   - Calls `child.try_wait()` in every probe iteration.
   - If the child exited, reads stderr and returns immediately with the
     exit code and stderr tail instead of waiting 15s.

## 4. Regression Tests

**File**: `crates/harness-cli/src/tui/bootstrap_tests.rs` (new, 9 tests)

| Test | What it verifies |
|---|---|
| `path_inside_git_worktree_detects_git_repo` | Subdirectory of a git repo is detected |
| `path_inside_git_worktree_false_for_non_git_dir` | Non-git directory is not flagged |
| `safe_worktree_root_returns_default_when_not_in_git` | Default `target/tmp` returned when safe |
| `safe_worktree_root_avoids_git_worktree_for_git_repo` | Fallback path outside git repo when repo IS a git worktree |
| `read_child_stderr_captures_output` | stderr text is captured from a piped child |
| `read_child_stderr_returns_none_when_no_output` | `None` when child produced no stderr |
| `read_child_stderr_truncates_to_max_bytes` | Output truncated to `max_bytes` |
| `try_wait_detects_immediate_exit` | `try_wait` returns `Some(status)` for exited child |
| `try_wait_returns_none_for_running_process` | `try_wait` returns `None` for live child |

All tests use real processes (`cmd`/`sh`) and real `git init` — no mocks.

## 5. Quality Gates

| Gate | Command | Result |
|---|---|---|
| Format | `cargo fmt --all -- --check` | PASS |
| Clippy | `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| Tests | `cargo test --workspace` (TEMP=C:\Temp) | PASS (635 passed, 0 failed) |
| Build | `cargo build --workspace` | PASS |

### Test environment note

When `TEMP` points inside the git repository
(`E:\General-harness\.scratch\tmp`), 12 pre-existing `harness-runtime`
tests fail because their temp directories are inside the git worktree.
This is an environmental issue, not a regression — the same tests pass
when `TEMP=C:\Temp` (outside the repo). The `HARNESS_WORKTREE_ROOT`
environment variable must also point outside the repo.

## 6. Files Changed

| File | Change |
|---|---|
| `crates/harness-cli/src/commands/supervisor.rs` | Return `Child`, pipe stderr |
| `crates/harness-cli/src/main.rs` | Update callers for new return type |
| `crates/harness-cli/src/tui/mod.rs` | `safe_worktree_root`, early-exit detection, 3 helper functions |
| `crates/harness-cli/src/tui/bootstrap_tests.rs` | New — 9 regression tests |
| `docs/I8B_RUNTIME_CLOSURE_DIAGNOSIS.md` | New — Phase 1 investigation document |
| `verification/I8B_RUNTIME_CLOSURE_REPORT.md` | This file |

## 7. Closure Checklist

- [x] Root cause investigated (Phase 1, no code changes during investigation)
- [x] Root cause documented (diagnosis document)
- [x] Failure reproduced with process evidence
- [x] Single root cause hypothesis confirmed
- [x] Minimal production fix implemented (3 files)
- [x] Regression tests added (9 tests, real processes)
- [x] `cargo fmt --check` PASS
- [x] `cargo clippy -D warnings` PASS
- [x] `cargo test --workspace` PASS
- [x] `cargo build --workspace` PASS
- [x] No timeout increase (15s cap unchanged)
- [x] No I1–I7 / I8A domain semantics modified
- [x] No real LLM calls
- [x] No TUI rewrite
- [x] Git worktree clean after commit
