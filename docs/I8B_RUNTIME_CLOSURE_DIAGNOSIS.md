# I8B Runtime Closure — Root Cause Diagnosis

> Phase 1 investigation document. No code changes were made during this phase.

## Observed Failure

**Symptom**: `harness-cli` (no-arg, on a real TTY) compiles and starts, auto-spawns
a Supervisor child, but the health probe never succeeds within 15 seconds:

```
Supervisor not reachable — starting it (harness supervisor start)...
Supervisor started (PID: 9788) for state directory: default
Repo root: E:\General-harness
Use 'harness supervisor status' to check health
Error: SupervisorBootstrap("supervisor did not become healthy within 15s")
```

**Classification**: BUILD PASS, runtime startup failure. The TUI never appears.

## Startup Call Graph

```
main() [main.rs:46]
 └─ no-arg TUI entry [main.rs:50]
     └─ tui::run_tui(options) [tui/mod.rs:50]
         └─ ensure_supervisor(options) [tui/mod.rs:58]
             ├─ client.ping() → fails (no pipe)
             ├─ cmd_supervisor_start(db_path, "default", repo_root, None, None, false)
             │   [commands/supervisor.rs:42]
             │   ├─ Database::open(db_path)  ← OK
             │   ├─ std::env::current_exe()  ← correct binary
             │   ├─ spawn child: <exe> supervisor run --state-dir default --db <db_path> --repo <cwd>
             │   │   creation_flags = CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS
             │   │   stdout/stderr = NOT piped (inherited / lost with DETACHED_PROCESS)
             │   └─ return Ok(())  ← returns immediately, no child handle
             │
             └─ health probe loop (250ms → 5s backoff, 15s cap)
                 └─ client.ping() → fails every time → timeout
```

### Child process path (`supervisor run`):

```
main() [main.rs:44]  — child receives: supervisor run --state-dir default --db target/data/harness.db --repo E:\General-harness
 ├─ is_supervisor_run = true → skips IPC dispatch
 ├─ RunContext::create(&repo_root, code_head, true)  ← OK, does NOT change cwd
 ├─ Database::open(&db_path)  ← OK
 ├─ worktree_root = parse_flag("--worktree-root")  ← None
 │   .or(HARNESS_WORKTREE_ROOT env)               ← None
 │   .unwrap_or(repo_root.join("target/tmp"))      ← E:\General-harness\target\tmp
 ├─ bootstrap_production_graph(pool, &worktree_root, &repo_root, run_context)
 │   [bootstrap.rs:48]
 │   ├─ AgentDiscoveryService::discover()  ← scans PATH, runs --version/--help probes
 │   └─ ProductionGraph::build_with_adapter(pool, &worktree_root, &repo_root, ...)
 │       [production_graph.rs:142]
 │       └─ WorktreeManager::new(pool, inspector, worktree_root, ...)
 │           [worktree/manager.rs:41]
 │           ├─ find_git_ancestor(worktree_root)  ← finds E:\General-harness\.git
 │           └─ return Err("worktree root ... is inside a git worktree")
 │
 └─ eprintln!("fatal: bootstrap failed: worktree manager: ...")
     run_context.shutdown(false)
     std::process::exit(1)  ← CHILD EXITS IMMEDIATELY
```

## Identity Inputs

| Identity | TUI Client (`ensure_supervisor`) | Supervisor Child (`supervisor run`) | Status |
|---|---|---|---|
| Named Pipe | `DEFAULT_ENDPOINT = "harness-supervisor"` → `\\.\pipe\harness-supervisor` | `SupervisorConfig::default().ipc_endpoint = "harness-supervisor"` → `\\.\pipe\harness-supervisor` | **CONSISTENT** |
| State directory | `"default"` (hardcoded in `ensure_supervisor`) | `"default"` (from `--state-dir`, `SupervisorConfig::default()`) | **CONSISTENT** |
| DB path | `"target/data/harness.db"` (relative, resolved from cwd) | `"target/data/harness.db"` (from `--db`, relative, child inherits cwd) | **CONSISTENT** |
| Child executable | `std::env::current_exe()` | (same binary) | **CORRECT** |
| Repo root | `Some(cwd)` → passed via `--repo` | Parsed from `--repo` flag | **CONSISTENT** |
| Worktree root | `None` → child defaults to `repo_root/target/tmp` | `repo_root/target/tmp` (inside git repo) | **ROOT CAUSE** |

## Potential Failure Classes

| Class | Hypothesis | Evidence | Verdict |
|---|---|---|---|
| A. Child exits immediately | WorktreeManager rejects worktree root inside git repo | `find_git_ancestor` finds `.git` at `E:\General-harness`; `WorktreeManager::new` returns error; child calls `exit(1)` | **CONFIRMED — ROOT CAUSE** |
| B. Child alive but slow | Agent discovery probes take >15s | `PROBE_TIMEOUT = 15s` per probe; `claude --version` + `claude --help` could take 4-10s | Possible secondary factor, but child crashes BEFORE reaching IPC server due to (A) |
| C. Wrong binary via PATH | `Command::new("harness")` uses stale binary | Code uses `std::env::current_exe()` — correct | **EXCLUDED** |
| D. Identity mismatch | Start/health use different pipe names or state dirs | Both use `"harness-supervisor"` and `"default"` | **EXCLUDED** |
| E. DB path mismatch | Parent/child open different DBs | Both use `"target/data/harness.db"` (relative, child inherits cwd) | **EXCLUDED** |
| F. Lease/lock blocks startup | Existing lease prevents new supervisor | Child crashes before reaching ownership acquisition | **EXCLUDED (for this failure)** |
| G. Detached process issues | stderr lost, early exit invisible | `DETACHED_PROCESS` + no piped stderr → error messages lost | **CONFIRMED — diagnostic gap** |

## Evidence

### E1: Process evidence (reproduction)

```
=== Starting supervisor ===
=== supervisor start returned in 00:00:00.1724776 ===

=== Process check (immediate) ===
(no harness-cli processes)

=== Process check (after 5s) ===
(no harness-cli processes)
```

The child process exits within 0.17 seconds. No harness-cli process is alive
at any point after `supervisor start` returns.

### E2: supervisor status output

```
warning: Supervisor unavailable, using offline database (read-only)
fatal: bootstrap failed: worktree manager: ["workspace_error"]
  worktree root E:\General-harness\target/tmp is inside a git worktree
  (E:\General-harness); configure a harness data directory instead
  (retryable=false, source=System)
```

This is the same error the child encounters. The `supervisor status` command
also falls through to `bootstrap_production_graph` when IPC is unavailable,
exposing the exact crash reason.

### E3: Git verification

```
$ git rev-parse --is-inside-work-tree
true
$ git rev-parse --show-toplevel
E:/General-harness
```

`E:\General-harness` is a git repository. Any path under it (including
`target/tmp`) is inside the git worktree.

### E4: WorktreeManager rejection logic

```rust
// worktree/manager.rs:48-53
if let Some(ancestor) = crate::artifact::find_git_ancestor(worktree_root) {
    return Err(ws_err(format!(
        "worktree root {} is inside a git worktree ({}); configure a harness data directory instead",
        worktree_root.display(),
        ancestor.display()
    )));
}
```

`find_git_ancestor` walks up from the given path and returns the first ancestor
that contains a `.git` directory. It stops at the user's home directory.

### E5: ensure_supervisor blind wait

```rust
// tui/mod.rs:80-94
while waited < cap {
    tokio::time::sleep(delay).await;
    waited += delay;
    if client.ping().await.unwrap_or(false) {
        return Ok(());
    }
    delay = (delay * 2).min(std::time::Duration::from_secs(5));
}
Err(TuiError::SupervisorBootstrap(
    "supervisor did not become healthy within 15s".to_string(),
))
```

The loop only checks `client.ping()`. It never checks whether the child process
is still alive. A child that crashes at t=0.17s causes a 15s blind wait.

### E6: Child stderr is lost

```rust
// commands/supervisor.rs:89-96
#[cfg(windows)]
{
    use std::os::windows::process::CommandExt;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x00000200;
    const DETACHED_PROCESS: u32 = 0x00000008;
    cmd.creation_flags(CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS);
}
```

With `DETACHED_PROCESS`, the child has no console. `eprintln!` output goes
nowhere. `cmd_supervisor_start` does not pipe stdout/stderr, so the child's
crash message is invisible to the parent.

## Root Cause Summary

**Primary**: The `supervisor run` child process crashes during
`bootstrap_production_graph` because the default worktree root
(`repo_root/target/tmp`) is inside the git repository, which
`WorktreeManager::new()` rejects. The child exits immediately (within ~0.2s),
but `ensure_supervisor` does not detect the early exit and waits the full 15s
before reporting a timeout.

**Secondary (diagnostic gap)**: `cmd_supervisor_start` spawns the child with
`DETACHED_PROCESS` and does not pipe stderr. The child's crash message is
lost, making the timeout error the only symptom the user sees.

**Identity**: All identity inputs (Named Pipe, state directory, DB path,
binary path, repo root) are consistent between the TUI client and the
Supervisor child. The failure is NOT an identity mismatch.

**Timeout**: The 15s timeout is a **symptom**, not the root cause. The child
crashes at ~0.2s, not at 15s. Increasing the timeout would not help.
