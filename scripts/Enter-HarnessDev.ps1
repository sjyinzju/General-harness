# Enter-HarnessDev.ps1
# Repo-local development environment script.
# Sets scratch paths, CARGO_TARGET_DIR, CARGO_INCREMENTAL=0 for this session.
# Never modifies Windows global or user environment.
#
# Usage:
#   . .\scripts\Enter-HarnessDev.ps1
#
# Or from repo root:
#   . .\scripts\Enter-HarnessDev.ps1 -RepoRoot .

param(
    [string]$RepoRoot = ""
)
if (-not $RepoRoot) {
    $RepoRoot = Join-Path $PSScriptRoot ".."
}

$ErrorActionPreference = "Stop"

# Resolve repo root (do NOT hardcode username)
$RepoRoot = (Resolve-Path $RepoRoot).Path
Write-Host "=== General-Harness Dev Environment ===" -ForegroundColor Cyan
Write-Host "Repo Root: $RepoRoot"

# ── Define scratch paths ──────────────────────────────────────────
$ScratchRoot = Join-Path $RepoRoot ".scratch"
$ScratchTmp   = Join-Path $ScratchRoot "tmp"
$CargoTarget  = Join-Path $RepoRoot "target\scratch"

# ── Create directories ────────────────────────────────────────────
$dirs = @($ScratchRoot, $ScratchTmp, $CargoTarget)
foreach ($d in $dirs) {
    if (-not (Test-Path $d)) {
        New-Item -ItemType Directory -Path $d -Force | Out-Null
        Write-Host "[create] $d"
    }
}

# ── Write ownership marker ────────────────────────────────────────
$markerPath = Join-Path $ScratchRoot ".general-harness-scratch"
if (-not (Test-Path $markerPath)) {
    @{
        schema_version = 1
        kind           = "general-harness-scratch"
        repo_root      = $RepoRoot
        created_at     = (Get-Date).ToUniversalTime().ToString("o")
    } | ConvertTo-Json -Depth 3 | Out-File -FilePath $markerPath -Encoding utf8
}

# ── Set environment (this session only) ───────────────────────────
$env:HARNESS_SCRATCH_ROOT = $ScratchRoot
$env:TEMP                 = $ScratchTmp
$env:TMP                  = $ScratchTmp
$env:TMPDIR               = $ScratchTmp
$env:CARGO_TARGET_DIR     = $CargoTarget
$env:CARGO_INCREMENTAL    = "0"

# ── Print effective environment ───────────────────────────────────
Write-Host ""
Write-Host "Effective Environment:" -ForegroundColor Green
Write-Host "  HARNESS_SCRATCH_ROOT = $env:HARNESS_SCRATCH_ROOT"
Write-Host "  TEMP                 = $env:TEMP"
Write-Host "  TMP                  = $env:TMP"
Write-Host "  TMPDIR               = $env:TMPDIR"
Write-Host "  CARGO_TARGET_DIR     = $env:CARGO_TARGET_DIR"
Write-Host "  CARGO_INCREMENTAL    = $env:CARGO_INCREMENTAL"
Write-Host ""
Write-Host "=== Environment Ready ===" -ForegroundColor Cyan
