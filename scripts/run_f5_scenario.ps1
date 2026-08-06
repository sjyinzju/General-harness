# F5 Fault Scenario Runner
# Runs a single F5 scenario: Verification PASS → crash before Candidate → recovery
param(
    [string]$RunId = "f5-run-1",
    [string]$WorkRoot = "E:\General-harness\target\fault-runs"
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$RUN_DIR = Join-Path $WorkRoot $RunId
$DB = Join-Path $RUN_DIR "harness.db"
$REPO = Join-Path $RUN_DIR "repo"
$WT_ROOT = Join-Path $RUN_DIR "wt"
$STATE_A = "f5-state-a"
$STATE_B = "f5-state-b"
$FP_DIR = Join-Path $RUN_DIR "failpoints"
$HARNESS = "E:\General-harness\target\debug\harness.exe"
$CODE_HEAD = (git -C E:\General-harness rev-parse HEAD)

Write-Host "=== F5 Scenario Runner ==="
Write-Host "Run: $RunId"
Write-Host "Code: $CODE_HEAD"
Write-Host "Dir: $RUN_DIR"

# Clean and create directories
Remove-Item -Recurse -Force $RUN_DIR -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $RUN_DIR | Out-Null
New-Item -ItemType Directory -Force -Path $REPO | Out-Null
New-Item -ItemType Directory -Force -Path $WT_ROOT | Out-Null
New-Item -ItemType Directory -Force -Path $FP_DIR | Out-Null

# Setup HARNESS_FAILPOINT_DIR for isolated failpoints
$env:HARNESS_FAILPOINT_DIR = $FP_DIR

# Initialize git repo
Push-Location $REPO
git init -b main . 2>$null
git config user.email "f5@test"
git config user.name "F5-Test"
New-Item -ItemType Directory -Force -Path src | Out-Null
@"
// F5 test fixture
pub fn add(a: i32, b: i32) -> i32 { a + b }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_add() { assert_eq!(add(2, 3), 5); }
}
"@ | Out-File -Encoding utf8 src\lib.rs
@"
[package]
name = "f5-fixture"
version = "0.1.0"
edition = "2021"
"@ | Out-File -Encoding utf8 Cargo.toml
git add . 2>$null
git commit -q -m "initial" 2>$null
Pop-Location

# Initialize DB
Write-Host "Initializing database..."
$env:HARNESS_DETERMINISTIC_MODE = "1"
$env:HARNESS_FAILPOINT_ENABLE = "1"

# Use a quick db init by running harness with a throwaway command
$init_output = & $HARNESS --standalone --db $DB goal list 2>&1
Write-Host "DB init: $init_output"

# Create the goal directly via standalone mode
$GOAL_ID = "g-sys-F5-$(Get-Random)"
$GOAL_SPEC = @{
    goal_id = $GOAL_ID
    title = "F5 Crash Recovery Test"
    objective = "Verify F5 recovery correctness"
    repository_id = "test-repo"
    target_ref = "refs/heads/main"
    initial_base_head = $CODE_HEAD
    revision = 1
    success_criteria = @(
        @{
            criterion_id = "c1"
            description = "Recovery completes successfully"
            required = $true
            evidence_policy = "task_terminal_result"
            verification_policy = "existence_only"
            subjectivity = "objective"
        }
    )
    budget = @{
        max_total_tasks = 1
        max_plan_revisions = 3
        max_consecutive_failures = 2
        max_no_progress_iterations = 5
    }
    approval_policy = @{ require_approval_for = @() }
    created_by = @{ kind = "system"; id = "acceptance" }
    non_goals = @()
} | ConvertTo-Json -Compress

Write-Host "Creating goal: $GOAL_ID"
$goal_output = & $HARNESS --standalone --db $DB goal create $GOAL_SPEC 2>&1
Write-Host "Goal create: $goal_output"

# Release F1-F4 so the goal can reach F5
$FP_DIR_PATH = $FP_DIR
$EARLY_FPS = @(
    "f1_after_goal_persisted_before_planning",
    "f2_after_plan_revision_committed_before_task_dispatch",
    "f3_after_task_loop_committed_before_executor_spawn",
    "f4_after_executor_result_committed_before_verification"
)
foreach ($fp in $EARLY_FPS) {
    $release_file = Join-Path $FP_DIR_PATH "$fp.release"
    $timestamp = Get-Date -Format "o"
    $timestamp | Out-File -Encoding utf8 $release_file
    Write-Host "Pre-released: $fp"
}

# Start Supervisor A
Write-Host "Starting Supervisor A..."
$proc_a = Start-Process -FilePath $HARNESS -ArgumentList @(
    "supervisor", "run",
    "--state-dir", $STATE_A,
    "--db", $DB,
    "--repo", $REPO,
    "--worktree-root", $WT_ROOT,
    "--code-head", $CODE_HEAD
) -NoNewWindow -PassThru -EnvironmentVariables @{
    HARNESS_FAILPOINT_ENABLE = "1"
    HARNESS_DETERMINISTIC_MODE = "1"
    HARNESS_FAILPOINT_DIR = $FP_DIR_PATH
}

Write-Host "Supervisor A PID: $($proc_a.Id)"

# Wait for F5 failpoint hit
$F5_NAME = "f5_after_verification_pass_committed_before_candidate"
$F5_HIT_FILE = Join-Path $FP_DIR_PATH "$F5_NAME.hit"
Write-Host "Waiting for F5 hit at: $F5_HIT_FILE"

$hit_timeout = 120
$hit_count = 0
while (-not (Test-Path $F5_HIT_FILE) -and $hit_count -lt $hit_timeout) {
    Start-Sleep -Seconds 1
    $hit_count++
    if ($hit_count % 10 -eq 0) {
        Write-Host "  Still waiting... ($hit_count s)"
        # Check if Supervisor A is alive
        if ($proc_a.HasExited) {
            Write-Host "ERROR: Supervisor A exited early (code: $($proc_a.ExitCode))"
            break
        }
    }
}

if (Test-Path $F5_HIT_FILE) {
    $hit_content = Get-Content $F5_HIT_FILE
    Write-Host "F5 HIT at: $hit_content"
} else {
    Write-Host "ERROR: F5 failpoint not hit within ${hit_timeout}s"
    Stop-Process -Id $proc_a.Id -Force -ErrorAction SilentlyContinue
    exit 1
}

# Pre-crash DB assertions
Write-Host "Pre-crash checks..."
$cand_count_pre = & $HARNESS --standalone --db $DB goal check-table --table candidate_snapshots 2>&1 |
    Select-String "COUNT" | ForEach-Object { $_.Line }
Write-Host "Candidates before crash: $cand_count_pre"

# Verify verification PASS existed
# Check for executor observation
$exec_obs = & $HARNESS --standalone --db $DB goal list-observations $GOAL_ID 2>&1
Write-Host "Observations before crash: $exec_obs"

# Kill Supervisor A
Write-Host "Killing Supervisor A..."
Stop-Process -Id $proc_a.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2
Write-Host "Supervisor A terminated"

# Wait for lease expiry
Write-Host "Waiting for lease expiry (35s)..."
Start-Sleep -Seconds 35

# Release F5 + later failpoints
$LATER_FPS = @(
    $F5_NAME,
    "f6_after_review_approved_committed_before_controlled_commit",
    "f7_after_controlled_commit_created_before_integration_enqueue",
    "f8_after_integration_result_committed_before_goal_observation",
    "f9_after_goal_observation_committed_before_evaluator",
    "f10_after_assessment_committed_before_completion_policy"
)
foreach ($fp in $LATER_FPS) {
    $release_file = Join-Path $FP_DIR_PATH "$fp.release"
    $timestamp = Get-Date -Format "o"
    $timestamp | Out-File -Encoding utf8 $release_file
    Write-Host "Released: $fp"
}

Start-Sleep -Seconds 3

# Start Supervisor B
Write-Host "Starting Supervisor B..."
$proc_b = Start-Process -FilePath $HARNESS -ArgumentList @(
    "supervisor", "run",
    "--state-dir", $STATE_B,
    "--db", $DB,
    "--repo", $REPO,
    "--worktree-root", $WT_ROOT,
    "--code-head", $CODE_HEAD
) -NoNewWindow -PassThru -EnvironmentVariables @{
    HARNESS_FAILPOINT_ENABLE = "1"
    HARNESS_DETERMINISTIC_MODE = "1"
    HARNESS_FAILPOINT_DIR = $FP_DIR_PATH
}

Write-Host "Supervisor B PID: $($proc_b.Id)"

# Wait for goal to reach terminal state
Write-Host "Waiting for goal to complete..."
$goal_timeout = 120
$goal_count = 0
$goal_succeeded = $false
while ($goal_count -lt $goal_timeout) {
    Start-Sleep -Seconds 3
    $goal_count += 3
    $state_output = & $HARNESS --standalone --db $DB goal status $GOAL_ID 2>&1
    Write-Host "Goal state ($goal_count s): $state_output"
    if ($state_output -match "succeeded") {
        $goal_succeeded = $true
        Write-Host "GOAL SUCCEEDED!"
        break
    }
    if ($state_output -match "failed") {
        Write-Host "GOAL FAILED!"
        break
    }
    if ($proc_b.HasExited) {
        Write-Host "Supervisor B exited (code: $($proc_b.ExitCode))"
        break
    }
}

# Collect post-recovery evidence
Write-Host "`n=== Post-Recovery Evidence ==="
$cand_count_post = & $HARNESS --standalone --db $DB goal check-table --table candidate_snapshots 2>&1
Write-Host "Candidates: $cand_count_post"

$review_count = & $HARNESS --standalone --db $DB goal check-table --table review_requests 2>&1
Write-Host "Reviews: $review_count"

$commit_count = & $HARNESS --standalone --db $DB goal check-table --table commit_candidates 2>&1
Write-Host "Commits: $commit_count"

$integration_count = & $HARNESS --standalone --db $DB goal check-table --table integration_requests 2>&1
Write-Host "Integrations: $integration_count"

$obs_count = & $HARNESS --standalone --db $DB goal check-table --table goal_observations 2>&1
Write-Host "Observations: $obs_count"

Write-Host "`nGoal Succeeded: $goal_succeeded"

# Cleanup
Stop-Process -Id $proc_b.Id -Force -ErrorAction SilentlyContinue
Write-Host "Supervisor B terminated"

if ($goal_succeeded) {
    Write-Host "`n=== F5 PASS ==="
    exit 0
} else {
    Write-Host "`n=== F5 FAIL ==="
    exit 1
}
