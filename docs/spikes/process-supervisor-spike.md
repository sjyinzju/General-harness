# Process Supervisor Spike Report

> **日期**: 2026-07-15
> **平台**: Windows 11 (x86_64)
> **Rust**: 1.95.0

---

## Test Environment

- Windows 11 Home China 10.0.26200
- PowerShell 5.1
- tokio 1.52.3

## Results

| # | Test | Result | Notes |
|---|------|:---:|------|
| 1 | Spawn subprocess + capture stdout | ✅ PASS | `cmd /c echo` works, stdout captured via `Stdio::piped()` |
| 2 | Stdin write + read response | ✅ PASS | `powershell Read-Host` reads stdin pipe correctly |
| 3 | Stderr capture | ✅ PASS | stderr separated via `Stdio::piped()` |
| 4 | Timeout | ✅ PASS | `tokio::time::timeout(3s)` triggers, `taskkill /T /F` kills process tree |
| 5 | Cancellation (kill) | ✅ PASS | `taskkill /PID /T /F` terminates child, `child.wait()` confirms exit |
| 6 | Exit code detection | ✅ PASS | `exit /b 42` → `status.code() = Some(42)` |

## Implementation Approach

- `tokio::process::Command` for async subprocess spawn
- `Stdio::piped()` for stdout/stderr capture
- `tokio::time::timeout()` for timeout enforcement
- Windows: `taskkill /PID /T /F` for process tree termination
- `child.id()` for PID retrieval

## Platform Limitations

- Windows `taskkill /T` terminates child process tree but only if processes share the same console
- No direct Job Object API used yet — `taskkill` is a reasonable first approach
- Unix (`kill(-pgid)`) not yet tested

## Impact on process-ownership-model.md

- `tokio::process` + `taskkill` combination is sufficient for Foundation
- Watchdog subprocess can use `taskkill /T /F` (Win) or `kill(-pgid)` (Unix) to clean up Agent tree after supervisor crash
- Recommendation: Foundation uses `taskkill`/`kill` approach; Production Release adds Job Objects/cgroups for robustness
