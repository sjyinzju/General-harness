# cleanup-harness-artifacts.ps1
# I4.5 Temporary Artifact Lifecycle: Manual cleanup wrapper.
#
# Usage:
#   .\scripts\cleanup-harness-artifacts.ps1 [-DryRun] [-Apply]
#
# Behaviour:
#   - DryRun (default): Reports what WOULD be cleaned without deleting.
#   - Apply: Executes guaranteed-safe deletion of stale owned artifacts.
#   - NEVER deletes: unmanaged target dirs, repo root, system TEMP root,
#     user home, .git, shared cargo cache.
#
# This is a thin wrapper around `harness cleanup` CLI command.

param(
    [switch]$Apply = $false,
    [string]$RepoRoot = $PSScriptRoot\..
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path $RepoRoot).Path

Write-Host "=== Harness Artifact Cleanup ===" -ForegroundColor Cyan
Write-Host "Repo Root: $RepoRoot"
Write-Host ""

# Build harness-cli if needed.
$cliPath = Join-Path $RepoRoot "target\debug\harness.exe"
if (-not (Test-Path $cliPath)) {
    Write-Host "[build] Building harness-cli..."
    Push-Location $RepoRoot
    cargo build -p harness-cli 2>&1 | Out-Null
    Pop-Location
    if (-not (Test-Path $cliPath)) {
        Write-Error "Failed to build harness-cli"
        exit 1
    }
}

# Run cleanup.
$env:HARNESS_DB = Join-Path $RepoRoot "harness.db"

$args = @("cleanup")
if ($Apply) {
    $args += "--apply"
    Write-Host "[mode] APPLY — stale owned artifacts will be deleted."
}
else {
    Write-Host "[mode] DRY-RUN — no files will be deleted."
}
$args += "--repo"
$args += $RepoRoot

Write-Host ""
Write-Host "Running: harness $($args -join ' ')"
Write-Host ""

& $cliPath $args

Write-Host ""
Write-Host "=== Cleanup Complete ===" -ForegroundColor Cyan

if (-not $Apply) {
    Write-Host ""
    Write-Host "*** DRY RUN — use -Apply to execute deletions ***" -ForegroundColor Yellow
}
