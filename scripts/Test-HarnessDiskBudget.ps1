# Test-HarnessDiskBudget.ps1
# Lightweight disk budget check before builds.
# Fails fast if free space is below threshold or scratch dirs exceed hard limits.
# NEVER auto-deletes verification or other content to make space.
#
# Usage:
#   .\scripts\Test-HarnessDiskBudget.ps1
#   .\scripts\Test-HarnessDiskBudget.ps1 -MinFreeGB 30 -TargetScratchHardGB 50

param(
    [int]$MinFreeGB = 20,
    [int]$TargetScratchWarnGB = 30,
    [int]$TargetScratchHardGB = 40,
    [int]$DotScratchWarnGB = 10,
    [int]$DotScratchHardGB = 20,
    [string]$RepoRoot = ""
)
if (-not $RepoRoot) {
    $RepoRoot = Join-Path $PSScriptRoot ".."
}

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path $RepoRoot).Path

Write-Host "=== Harness Disk Budget Check ===" -ForegroundColor Cyan
$errors = @()
$warnings = @()

# ── Free space on repo disk ───────────────────────────────────────
$driveLetter = (Get-Item $RepoRoot).PSDrive.Name
$drive = Get-PSDrive -Name $driveLetter
$freeGB = [math]::Round($drive.Free / 1GB, 1)
Write-Host "Drive ${driveLetter}: Free = ${freeGB} GB, Min = ${MinFreeGB} GB"

if ($freeGB -lt $MinFreeGB) {
    $errors += "FREE_SPACE: ${freeGB} GB < ${MinFreeGB} GB minimum on drive ${driveLetter}:"
}

# ── System drive check ────────────────────────────────────────────
$sysDrive = (Get-Item $env:SystemRoot).PSDrive.Name
if ($sysDrive -ne $driveLetter) {
    Write-Host "System drive (${sysDrive}:) is NOT the repo drive (${driveLetter}:) — OK"
} else {
    Write-Host "Repo is on system drive — build tree will consume system disk"
}

# ── Helper: get dir size in GB ────────────────────────────────────
function Get-DirSizeGB {
    param([string]$Path)
    if (-not (Test-Path $Path)) { return 0 }
    try {
        $bytes = (Get-ChildItem -Path $Path -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
        return [math]::Round($bytes / 1GB, 2)
    } catch {
        return 0
    }
}

# ── target\scratch ────────────────────────────────────────────────
$targetScratch = Join-Path $RepoRoot "target\scratch"
$targetSize = Get-DirSizeGB $targetScratch
Write-Host "target\scratch: ${targetSize} GB (warn=${TargetScratchWarnGB}, hard=${TargetScratchHardGB})"

if ($targetSize -gt $TargetScratchHardGB) {
    $errors += "TARGET_SCRATCH_HARD: ${targetSize} GB > ${TargetScratchHardGB} GB — STOP, do not build"
} elseif ($targetSize -gt $TargetScratchWarnGB) {
    $warnings += "TARGET_SCRATCH_WARN: ${targetSize} GB > ${TargetScratchWarnGB} GB — consider cleanup"
}

# ── .scratch ──────────────────────────────────────────────────────
$dotScratch = Join-Path $RepoRoot ".scratch"
$dotSize = Get-DirSizeGB $dotScratch
Write-Host ".scratch:       ${dotSize} GB (warn=${DotScratchWarnGB}, hard=${DotScratchHardGB})"

if ($dotSize -gt $DotScratchHardGB) {
    $errors += "DOT_SCRATCH_HARD: ${dotSize} GB > ${DotScratchHardGB} GB — STOP"
} elseif ($dotSize -gt $DotScratchWarnGB) {
    $warnings += "DOT_SCRATCH_WARN: ${dotSize} GB > ${DotScratchWarnGB} GB"
}

# ── Check for per-run cargo targets (must NOT exist) ──────────────
$targetDir = Join-Path $RepoRoot "target"
if (Test-Path $targetDir) {
    $badTargets = Get-ChildItem -Path $targetDir -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match '^(system-|delta-|canary-|fullrun-|run-)' } |
        Select-Object -ExpandProperty Name
    if ($badTargets) {
        $errors += "PER_RUN_CARGO_TARGET: Found run-specific target dirs: $($badTargets -join ', ') — must use target\scratch only"
    }
}

# ── Final verdict ─────────────────────────────────────────────────
Write-Host ""
if ($warnings.Count -gt 0) {
    Write-Host "WARNINGS:" -ForegroundColor Yellow
    foreach ($w in $warnings) { Write-Host "  $w" -ForegroundColor Yellow }
}
if ($errors.Count -gt 0) {
    Write-Host "ERRORS (FAIL FAST):" -ForegroundColor Red
    foreach ($e in $errors) { Write-Host "  $e" -ForegroundColor Red }
    Write-Host ""
    Write-Host "=== DISK BUDGET FAILED ===" -ForegroundColor Red
    exit 1
}
Write-Host "=== DISK BUDGET PASS ===" -ForegroundColor Green
