# Clear-HarnessScratch.ps1
# Safely clean harness scratch directories.
# ONLY deletes <.scratch> and <target\scratch> within the repo.
# NEVER touches .git, crates, verification, src, Cargo.toml, Cargo.lock.
# NEVER deletes outside repo root.
#
# Usage:
#   .\scripts\Clear-HarnessScratch.ps1 -RunTempOnly    # only .scratch\tmp
#   .\scripts\Clear-HarnessScratch.ps1 -AllScratch      # .scratch + target\scratch

param(
    [switch]$RunTempOnly,
    [switch]$AllScratch,
    [string]$RepoRoot = ""
)
if (-not $RepoRoot) {
    $RepoRoot = Join-Path $PSScriptRoot ".."
}

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path $RepoRoot).Path

Write-Host "=== Clear-HarnessScratch ===" -ForegroundColor Cyan
Write-Host "Repo Root: $RepoRoot"

# ── Safety: ownership marker must exist ───────────────────────────
$markerPath = Join-Path $RepoRoot ".scratch\.general-harness-scratch"
if (-not (Test-Path $markerPath)) {
    Write-Error "SAFETY BLOCK: No ownership marker found at $markerPath"
    Write-Error "Create it first via Enter-HarnessDev.ps1 or manually."
    exit 1
}

# Verify marker points back to this repo
try {
    $marker = Get-Content $markerPath -Raw | ConvertFrom-Json
    $markerRepo = (Resolve-Path $marker.repo_root).Path
    if ($markerRepo -ne $RepoRoot) {
        Write-Error "SAFETY BLOCK: Marker repo_root ($markerRepo) does not match actual ($RepoRoot)"
        exit 1
    }
} catch {
    Write-Error "SAFETY BLOCK: Cannot validate ownership marker: $_"
    exit 1
}

# ── Helper: safe delete only within repo ──────────────────────────
function Remove-ScratchDir {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        Write-Host "[skip] $Path (does not exist)"
        return
    }
    $resolved = (Resolve-Path $Path).Path
    # Must be child of repo root
    if (-not $resolved.StartsWith($RepoRoot, [StringComparison]::OrdinalIgnoreCase)) {
        Write-Error "SAFETY BLOCK: $resolved is outside repo root — refusing to delete"
        exit 1
    }
    # Must not be one of the protected dirs
    $protected = @(
        (Join-Path $RepoRoot ".git"),
        (Join-Path $RepoRoot "crates"),
        (Join-Path $RepoRoot "verification"),
        (Join-Path $RepoRoot "src"),
        (Join-Path $RepoRoot "scripts"),
        (Join-Path $RepoRoot "Cargo.toml"),
        (Join-Path $RepoRoot "Cargo.lock")
    )
    foreach ($p in $protected) {
        if ($resolved -eq $p) {
            Write-Error "SAFETY BLOCK: $resolved is protected — refusing to delete"
            exit 1
        }
    }
    Write-Host "[delete] $resolved"
    Remove-Item -Path $resolved -Recurse -Force -ErrorAction Stop
}

# ── Execute ───────────────────────────────────────────────────────
$scratchRoot = Join-Path $RepoRoot ".scratch"
$cargoScratch = Join-Path $RepoRoot "target\scratch"

if ($RunTempOnly) {
    Write-Host "Mode: RunTempOnly"
    $tmpDir = Join-Path $scratchRoot "tmp"
    if (Test-Path $tmpDir) {
        Remove-ScratchDir -Path $tmpDir
        # Recreate so next commands don't fail
        New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null
    }
    # Also clean run directories
    $runsDir = Join-Path $scratchRoot "runs"
    if (Test-Path $runsDir) {
        Remove-ScratchDir -Path $runsDir
        New-Item -ItemType Directory -Path $runsDir -Force | Out-Null
    }
} elseif ($AllScratch) {
    Write-Host "Mode: AllScratch"
    Remove-ScratchDir -Path $scratchRoot
    Remove-ScratchDir -Path $cargoScratch
} else {
    Write-Error "Specify -RunTempOnly or -AllScratch"
    exit 1
}

Write-Host ""
Write-Host "=== Cleanup Complete ===" -ForegroundColor Green
