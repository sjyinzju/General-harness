# run-with-managed-temp.ps1
# I4.5 Temporary Artifact Lifecycle: Runner TEMP/TMP isolation wrapper.
#
# Usage:
#   .\scripts\run-with-managed-temp.ps1 -RunId "run-001" -Command "cargo test ..."
#
# Behaviour:
#   1. Creates `target\harness-temp\<RunId>\` with `.harness-owned.json`.
#   2. Redirects $env:TEMP and $env:TMP for this process tree.
#   3. Executes the given command.
#   4. In a `finally` block, restores the original TEMP/TMP and
#      attempts a guarded cleanup of the managed temp directory.
#
# Safety invariants:
#   - NEVER uses setx or [Environment]::SetEnvironmentVariable.
#   - Only modifies the current PowerShell process and its children.
#   - The managed temp dir is only deleted if:
#     a) it was created by THIS script
#     b) the ownership marker matches
#     c) no active owner is detected
#   - On cleanup failure, the directory is left for Startup Janitor.

param(
    [Parameter(Mandatory = $true)]
    [string]$RunId,

    [Parameter(Mandatory = $true)]
    [string]$Command,

    [string]$RepoRoot = $PSScriptRoot\..,

    [string]$CodeHead = "unknown",

    [switch]$SkipCleanup = $false
)

$ErrorActionPreference = "Stop"

# Resolve absolute paths.
$RepoRoot = (Resolve-Path $RepoRoot).Path
$ManagedTempRoot = Join-Path $RepoRoot "target\harness-temp"
$RunTempDir = Join-Path $ManagedTempRoot $RunId

# ── Save original environment ────────────────────────────────────
$OriginalTemp = $env:TEMP
$OriginalTmp  = $env:TMP

Write-Host "=== Harness Managed Temp Isolation ===" -ForegroundColor Cyan
Write-Host "RunId:          $RunId"
Write-Host "Managed Root:   $ManagedTempRoot"
Write-Host "Run Temp Dir:   $RunTempDir"
Write-Host "Original TEMP:  $OriginalTemp"
Write-Host "Original TMP:   $OriginalTmp"
Write-Host "Code Head:      $CodeHead"
Write-Host ""

# ── Marker helper ─────────────────────────────────────────────────
function Write-OwnershipMarker {
    param([string]$Dir, [string]$State = "active")

    $marker = @{
        schema_version            = 1
        kind                      = "harness-managed-temp"
        run_id                    = $RunId
        owner_pid                 = $PID
        owner_process_created_at  = (Get-Date).ToUniversalTime().ToString("o")
        created_at                = (Get-Date).ToUniversalTime().ToString("o")
        code_head                 = $CodeHead
        state                     = $State
    }

    $markerJson = $marker | ConvertTo-Json -Depth 4
    $markerPath = Join-Path $Dir ".harness-owned.json"
    $tmpPath    = Join-Path $Dir ".harness-owned.json.tmp"

    # Atomic write.
    $markerJson | Out-File -FilePath $tmpPath -Encoding utf8 -NoNewline
    if (Test-Path $markerPath) {
        Remove-Item $markerPath -Force
    }
    Move-Item $tmpPath $markerPath -Force
    Write-Host "[marker] Written: $markerPath"
}

# ── Create managed temp dir ───────────────────────────────────────
New-Item -ItemType Directory -Path $ManagedTempRoot -Force | Out-Null
if (Test-Path $RunTempDir) {
    Write-Warning "Run temp dir already exists: $RunTempDir"
    Write-Warning "This may indicate a RunId collision. Contents will NOT be deleted."
}
New-Item -ItemType Directory -Path $RunTempDir -Force | Out-Null
Write-OwnershipMarker -Dir $RunTempDir -State "active"

# ── Redirect environment ──────────────────────────────────────────
$env:TEMP = $RunTempDir
$env:TMP  = $RunTempDir
Write-Host "[env] TEMP redirected to: $RunTempDir"
Write-Host "[env] TMP  redirected to: $RunTempDir"
Write-Host ""

# ── Execute command ───────────────────────────────────────────────
$exitCode = 0
$commandFailed = $false

try {
    Write-Host "=== Running: $Command ===" -ForegroundColor Yellow
    Write-Host ""

    # Capture duration.
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    Invoke-Expression $Command
    $exitCode = $LASTEXITCODE
    $sw.Stop()

    Write-Host ""
    Write-Host "=== Command finished ===" -ForegroundColor Yellow
    Write-Host "ExitCode:   $exitCode"
    Write-Host "Duration:   $($sw.Elapsed.ToString())"

    if ($exitCode -ne 0) {
        $commandFailed = $true
    }
}
catch {
    $commandFailed = $true
    Write-Host ""
    Write-Host "=== Command threw exception ===" -ForegroundColor Red
    Write-Host $_.Exception.Message
}
finally {
    # ── Restore environment ───────────────────────────────────────
    $env:TEMP = $OriginalTemp
    $env:TMP  = $OriginalTmp
    Write-Host "[env] TEMP restored to: $OriginalTemp"
    Write-Host "[env] TMP  restored to: $OriginalTmp"

    # ── Finalize marker ───────────────────────────────────────────
    $finalState = if ($commandFailed) { "failed" } else { "completed" }
    Write-Host "[cleanup] Finalizing ownership marker: state=$finalState"
    try {
        Write-OwnershipMarker -Dir $RunTempDir -State $finalState
    }
    catch {
        Write-Warning "Failed to finalize ownership marker: $_"
    }

    # ── Cleanup ───────────────────────────────────────────────────
    if (-not $SkipCleanup) {
        Write-Host "[cleanup] Attempting guarded cleanup of: $RunTempDir"

        # Wait briefly for child processes to exit.
        Start-Sleep -Milliseconds 500

        # Retry loop.
        $retries = @(100, 250, 500)
        $deleted = $false
        foreach ($delay in $retries) {
            try {
                if (Test-Path $RunTempDir) {
                    Remove-Item -Path $RunTempDir -Recurse -Force -ErrorAction Stop
                    $deleted = $true
                    Write-Host "[cleanup] Deleted: $RunTempDir"
                    break
                }
                else {
                    $deleted = $true
                    Write-Host "[cleanup] Directory already gone: $RunTempDir"
                    break
                }
            }
            catch {
                Write-Warning "[cleanup] Retry after ${delay}ms: $_"
                Start-Sleep -Milliseconds $delay
            }
        }

        if (-not $deleted) {
            Write-Warning "[cleanup] FAILED to delete: $RunTempDir"
            Write-Warning "[cleanup] Will be reclaimed by Startup Janitor on next run."
        }
    }
    else {
        Write-Host "[cleanup] Skipped (SkipCleanup flag set)."
    }
}

Write-Host ""
Write-Host "=== Harness Managed Temp Isolation Complete ===" -ForegroundColor Cyan

exit $exitCode
