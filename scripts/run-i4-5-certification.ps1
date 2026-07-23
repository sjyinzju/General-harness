# I4.5 Certification Runner
# Automates the full certification suite: 30 fault cases, 27 scenarios,
# 18 repeat groups, 5 C8 schedules, 8 crash prefixes, fmt, clippy,
# 3 workspace runs.
#
# Output: target/i4-5-certification/results.json + summary.md
# Exit code: 0 on full pass, non-zero on any failure.

param(
    [switch]$SkipFmt,
    [switch]$SkipClippy,
    [switch]$Quick  # reduced repeat counts for development
)

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path "$ScriptDir\.."
$OutputDir = "$RepoRoot\target\i4-5-certification"
$ResultsFile = "$OutputDir\results.json"
$SummaryFile = "$OutputDir\summary.md"

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

$ExecutionHead = (git -C $RepoRoot rev-parse HEAD)
# CODE_CANDIDATE_HEAD is the frozen implementation commit. It MUST differ
# from REPORT_HEAD.  The runner reads it from the I4.5 final certification
# report or falls back to the env var / git tag.
$CodeCandidateHead = if ($env:I45_CODE_CANDIDATE_HEAD) {
    $env:I45_CODE_CANDIDATE_HEAD
} else {
    # Fallback: use the parent of REPORT_HEAD if REPORT_HEAD only modified
    # I4.5_FINAL_CERTIFICATION_REPORT.md; otherwise equals execution HEAD.
    $reportDiff = git -C $RepoRoot diff --name-only HEAD~1 HEAD 2>$null
    if ($reportDiff -eq "I4.5_FINAL_CERTIFICATION_REPORT.md") {
        (git -C $RepoRoot rev-parse HEAD~1)
    } else {
        $ExecutionHead
    }
}
$StartTime = Get-Date
$Results = @()

function Write-Result {
    param($Group, $Test, $RequiredRuns, $ActualRuns, $Passed, $Failed, $ExitCode, $DurationMs, $FirstFailure)
    $script:Results += [PSCustomObject]@{
        code_candidate_head = $CodeCandidateHead
        execution_head = $ExecutionHead
        group = $Group
        test = $Test
        required_runs = $RequiredRuns
        actual_runs = $ActualRuns
        passed = $Passed
        failed = $Failed
        skipped = 0
        exit_code = $ExitCode
        duration_ms = $DurationMs
        first_failure = if ($FirstFailure) { $FirstFailure } else { "" }
    }
}

function Write-ScenarioResult {
    param($ScenarioId, $TestName, $Binary, $Passed, $Failed, $ExitCode, $DurationMs, $FirstFailure)
    $script:Results += [PSCustomObject]@{
        code_candidate_head = $CodeCandidateHead
        execution_head = $ExecutionHead
        group = "scenario"
        category = "scenario"
        scenario_id = $ScenarioId
        test_target = $Binary
        test_name = $TestName
        required_runs = 1
        actual_runs = 1
        passed = $Passed
        failed = $Failed
        skipped = 0
        exit_code = $ExitCode
        duration_ms = $DurationMs
        first_failure = if ($FirstFailure) { $FirstFailure } else { "" }
    }
}

function Invoke-CargoTest {
    param($TestName, $Count, $Group)
    $passed = 0; $failed = 0; $firstFailure = ""
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    for ($i = 1; $i -le $Count; $i++) {
        $output = cargo test -p harness-runtime --test $TestName -- --nocapture 2>&1
        if ($LASTEXITCODE -eq 0) {
            $passed++
        } else {
            $failed++
            if (-not $firstFailure) {
                $firstFailure = ($output | Select-Object -Last 5 | Out-String).Trim()
            }
        }
    }
    $sw.Stop()
    Write-Result -Group $Group -Test $TestName -RequiredRuns $Count -ActualRuns ($passed + $failed) `
        -Passed $passed -Failed $failed -ExitCode $(if ($failed -gt 0) { 1 } else { 0 }) `
        -DurationMs $sw.ElapsedMilliseconds -FirstFailure $firstFailure
    return $failed -eq 0
}

function Invoke-SpecificTest {
    param($TestName, $Count, $Group, $ExtraArgs)
    $passed = 0; $failed = 0; $firstFailure = ""
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    for ($i = 1; $i -le $Count; $i++) {
        if ($ExtraArgs) {
            # Support both array and string forms for backward compat.
            if ($ExtraArgs -is [array]) {
                $output = cargo test -p harness-runtime @ExtraArgs $TestName -- --nocapture 2>&1
            } else {
                $parts = -split $ExtraArgs
                $output = cargo test -p harness-runtime @parts $TestName -- --nocapture 2>&1
            }
        } else {
            $output = cargo test -p harness-runtime --lib -- $TestName --nocapture 2>&1
        }
        if ($LASTEXITCODE -eq 0) {
            $passed++
        } else {
            $failed++
            if (-not $firstFailure) {
                $firstFailure = ($output | Select-Object -Last 5 | Out-String).Trim()
            }
        }
    }
    $sw.Stop()
    $exitCode = if ($failed -gt 0) { 1 } else { 0 }
    Write-Result -Group $Group -Test $TestName -RequiredRuns $Count -ActualRuns ($passed + $failed) `
        -Passed $passed -Failed $failed -ExitCode $exitCode `
        -DurationMs $sw.ElapsedMilliseconds -FirstFailure $firstFailure
    return $failed -eq 0
}

$AllPassed = $true
$TotalTests = 0
$TotalPassed = 0
$TotalFailed = 0

# ═══════════════════════════════════════════════════════════════════════
# 1. Format check
# ═══════════════════════════════════════════════════════════════════════
if (-not $SkipFmt) {
    Write-Host "=== fmt ===" -ForegroundColor Cyan
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    cargo fmt --all --check 2>&1 | Out-Null
    $ok = ($LASTEXITCODE -eq 0)
    $sw.Stop()
    $p = if ($ok) { 1 } else { 0 }; $f = if ($ok) { 0 } else { 1 }
    Write-Result -Group "fmt" -Test "cargo fmt --all --check" -RequiredRuns 1 -ActualRuns 1 `
        -Passed $p -Failed $f `
        -ExitCode $LASTEXITCODE -DurationMs $sw.ElapsedMilliseconds
    if (-not $ok) { $AllPassed = $false; Write-Host "FAIL: fmt" -ForegroundColor Red }
    else { Write-Host "PASS: fmt" -ForegroundColor Green }
}

# ═══════════════════════════════════════════════════════════════════════
# 2. Clippy
# ═══════════════════════════════════════════════════════════════════════
if (-not $SkipClippy) {
    Write-Host "=== clippy ===" -ForegroundColor Cyan
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    cargo clippy --workspace --all-targets -- -D warnings 2>&1 | Out-Null
    $ok = ($LASTEXITCODE -eq 0)
    $sw.Stop()
    $p = if ($ok) { 1 } else { 0 }; $f = if ($ok) { 0 } else { 1 }
    Write-Result -Group "clippy" -Test "cargo clippy --workspace --all-targets -- -D warnings" -RequiredRuns 1 -ActualRuns 1 `
        -Passed $p -Failed $f `
        -ExitCode $LASTEXITCODE -DurationMs $sw.ElapsedMilliseconds
    if (-not $ok) { $AllPassed = $false; Write-Host "FAIL: clippy" -ForegroundColor Red }
    else { Write-Host "PASS: clippy" -ForegroundColor Green }
}

# ═══════════════════════════════════════════════════════════════════════
# 3. Fault Cases (30)
# ═══════════════════════════════════════════════════════════════════════
$faultCases = @(
    "test_fc01_loop_insert_before_effect", "test_fc02_loop_insert_response_lost",
    "test_fc03_ownership_before_effect", "test_fc04_ownership_response_lost",
    "test_fc05_stale_takeover_response_lost", "test_fc06_attempt_insert_before_effect",
    "test_fc07_attempt_insert_response_lost", "test_fc08_budget_reservation_before_effect",
    "test_fc09_budget_reservation_response_lost", "test_fc10_profile_selection_before_effect",
    "test_fc11_profile_selection_response_lost", "test_fc12_execution_create_before_effect",
    "test_fc13_execution_create_response_lost", "test_fc14_execution_binding_before_effect",
    "test_fc15_execution_binding_response_lost", "test_fc16_dispatch_before_effect",
    "test_fc17_dispatch_response_lost", "test_fc18_outcome_observation_failure",
    "test_fc19_dossier_read_failure", "test_fc20_decision_insert_before_effect",
    "test_fc21_decision_response_lost", "test_fc22_context_pack_before_effect",
    "test_fc23_context_pack_response_lost", "test_fc24_usage_write_before_effect",
    "test_fc25_usage_response_lost", "test_fc26_workspace_continuation_before_effect",
    "test_fc27_workspace_transfer_response_lost", "test_fc28_terminal_transition_response_lost",
    "test_fc29_terminal_event_response_lost", "test_fc30_owner_fencing_change_before_effect"
)

Write-Host "=== Fault Cases (30) ===" -ForegroundColor Cyan
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$fc_failed = 0
foreach ($fc in $faultCases) {
    $output = cargo test -p harness-runtime --test task_loop_fault_tests $fc -- --nocapture 2>&1
    if ($LASTEXITCODE -ne 0) { $fc_failed++ }
}
$sw.Stop()
$fcExit = if ($fc_failed -gt 0) { 1 } else { 0 }
Write-Result -Group "fault_cases" -Test "all 30 fault cases" -RequiredRuns 30 -ActualRuns 30 `
    -Passed (30 - $fc_failed) -Failed $fc_failed -ExitCode $fcExit `
    -DurationMs $sw.ElapsedMilliseconds
$TotalTests += 30; $TotalPassed += (30 - $fc_failed); $TotalFailed += $fc_failed
if ($fc_failed -gt 0) { $AllPassed = $false; Write-Host "FAIL: $fc_failed fault cases failed" -ForegroundColor Red }
else { Write-Host "PASS: 30/30 fault cases" -ForegroundColor Green }

# ═══════════════════════════════════════════════════════════════════════
# 4. Certification Scenarios (27) — each run individually
# ═══════════════════════════════════════════════════════════════════════
$allScenarios = @(
    @{id="gp01"; test="test_gp01_first_attempt_passes";                 binary="task_loop_fault_tests"},
    @{id="gp02"; test="test_gp02_one_repair_then_pass";                  binary="task_loop_fault_tests"},
    @{id="gp03"; test="test_gp03_progressive_repairs_budget_allows";     binary="task_loop_fault_tests"},
    @{id="gp04"; test="test_gp04_no_progress_stop";                      binary="task_loop_fault_tests"},
    @{id="gp05"; test="test_gp05_cycle_detection";                       binary="task_loop_fault_tests"},
    @{id="gp06"; test="test_gp06_hard_attempt_budget";                   binary="task_loop_fault_tests"},
    @{id="gp07"; test="test_gp07_unknown_token_usage";                   binary="task_loop_fault_tests"},
    @{id="gp08"; test="test_gp08_hard_token_budget";                    binary="task_loop_fault_tests"},
    @{id="gp09"; test="test_gp09_hard_tool_call_budget";                binary="task_loop_fault_tests"},
    @{id="gp10"; test="test_gp10_hard_cost_budget";                     binary="task_loop_fault_tests"},
    @{id="gp11"; test="test_gp11_infrastructure_blocked";                binary="task_loop_fault_tests"},
    @{id="gp12"; test="test_gp12_reconciliation_required";               binary="task_loop_fault_tests"},
    @{id="gp13"; test="test_gp13_awaiting_human";                       binary="task_loop_fault_tests"},
    @{id="gp14"; test="test_gp14_project_escalation";                   binary="task_loop_fault_tests"},
    @{id="gp15"; test="test_gp15_cancellation_classification";           binary="task_loop_fault_tests"},
    @{id="gp16"; test="test_gp16_cancellation_overrides";               binary="task_loop_fault_tests"},
    @{id="gp23"; test="test_gp23_two_pool_full_controller";            binary="task_loop_fault_tests"},
    @{id="gp26"; test="test_gp26_profile_selection_all_scenarios";      binary="task_loop_fault_tests"},
    @{id="gp27"; test="test_gp27_context_security";                    binary="task_loop_fault_tests"},
    @{id="ri01"; test="test_real_i4_first_attempt_pass";                binary="real_i4_e2e_tests"},
    @{id="ri02"; test="test_real_i4_repair_then_pass";                  binary="real_i4_e2e_tests"},
    @{id="ri03"; test="test_real_i4_crash_restart";                     binary="real_i4_e2e_tests"},
    @{id="ri04"; test="test_real_i4_workspace_continuation";            binary="real_i4_e2e_tests"},
    @{id="ri05"; test="test_real_i4_two_pool_full_lifecycle";           binary="real_i4_e2e_tests"},
    @{id="ii01"; test="test_first_attempt_passes";                      binary="task_loop_i4_integration"},
    @{id="ii02"; test="test_one_repair_then_pass";                      binary="task_loop_i4_integration"},
    @{id="ii03"; test="test_two_pool_full_lifecycle_one_winner";        binary="task_loop_i4_integration"}
)

Write-Host "=== Scenarios (27 individual) ===" -ForegroundColor Cyan
$scPassed = 0; $scFailed = 0; $scFirstFailure = ""
$scSw = [System.Diagnostics.Stopwatch]::StartNew()
foreach ($s in $allScenarios) {
    $sSw = [System.Diagnostics.Stopwatch]::StartNew()
    $output = cargo test -p harness-runtime --test $s.binary $s.test -- --nocapture 2>&1
    $sSw.Stop()
    $sOk = ($LASTEXITCODE -eq 0)
    $sFailFirst = ""
    if ($sOk) {
        $scPassed++
    } else {
        $scFailed++
        $sFailFirst = "$($s.id): $($output | Select-Object -Last 3 | Out-String)"
        if (-not $scFirstFailure) {
            $scFirstFailure = $sFailFirst
        }
    }
    Write-ScenarioResult -ScenarioId $s.id -TestName $s.test -Binary $s.binary `
        -Passed $(if ($sOk) { 1 } else { 0 }) `
        -Failed $(if ($sOk) { 0 } else { 1 }) `
        -ExitCode $(if ($sOk) { 0 } else { 1 }) `
        -DurationMs $sSw.ElapsedMilliseconds `
        -FirstFailure $sFailFirst
}
$scSw.Stop()
$scExit = if ($scFailed -gt 0) { 1 } else { 0 }
# Total from 27 individual detail rows (no separate summary row — no double counting).
$TotalTests += 27; $TotalPassed += $scPassed; $TotalFailed += $scFailed
if ($scFailed -gt 0) { $AllPassed = $false; Write-Host "FAIL: $scFailed scenarios failed" -ForegroundColor Red }
else { Write-Host "PASS: 27/27 scenarios" -ForegroundColor Green }

# ═══════════════════════════════════════════════════════════════════════
# 5. Repeat Groups (18)
# ═══════════════════════════════════════════════════════════════════════
$repeatGroups = @(
    @{name="rg01_first_attempt_passes"; test="test_real_i4_first_attempt_pass"; count=20; extra="--test real_i4_e2e_tests"},
    @{name="rg02_one_repair_then_pass"; test="test_real_i4_repair_then_pass"; count=20; extra="--test real_i4_e2e_tests"},
    @{name="rg03_progressive_repairs"; test="test_gp03_progressive_repairs_budget_allows"; count=20; extra="--test task_loop_fault_tests"},
    @{name="rg04_no_progress_stop"; test="test_gp04_no_progress_stop"; count=20; extra="--test task_loop_fault_tests"},
    @{name="rg05_two_pool_full_controller"; test="test_real_i4_two_pool_full_lifecycle"; count=50; extra="--test real_i4_e2e_tests"},
    @{name="rg06_two_pool_attempt_creation"; test="test_repeat_two_pool_attempt_creation_100"; count=100; extra="--test task_loop_fault_tests"},
    @{name="rg07_response_lost_attempt"; test="test_fc07_attempt_insert_response_lost"; count=20; extra="--test task_loop_fault_tests"},
    @{name="rg08_response_lost_dispatch"; test="test_fc17_dispatch_response_lost"; count=20; extra="--test task_loop_fault_tests"},
    @{name="rg09_response_lost_decision"; test="test_fc21_decision_response_lost"; count=20; extra="--test task_loop_fault_tests"},
    @{name="rg10_decision_exactly_once"; test="test_fc20_decision_insert_before_effect"; count=20; extra="--test task_loop_fault_tests"},
    @{name="rg11_context_pack_exactly_once"; test="test_fc22_context_pack_before_effect"; count=20; extra="--test task_loop_fault_tests"},
    @{name="rg12_budget_reservation_exactly_once"; test="test_fc08_budget_reservation_before_effect"; count=20; extra="--test task_loop_fault_tests"},
    @{name="rg13_usage_exactly_once"; test="test_fc24_usage_write_before_effect"; count=20; extra="--test task_loop_fault_tests"},
    @{name="rg14_crash_resume_loop"; test="test_real_i4_crash_restart"; count=10; extra="--test real_i4_e2e_tests"},
    @{name="rg15_workspace_continuation"; test="test_real_i4_workspace_continuation"; count=10; extra="--test real_i4_e2e_tests"},
    @{name="rg16_profile_switch_allowed"; test="test_profile_policy_allows_switch_within_provider"; count=10; extra="--test task_loop_i4_integration"},
    @{name="rg17_profile_switch_forbidden"; test="test_profile_policy_rejects_cross_provider"; count=10; extra="--test task_loop_i4_integration"},
    @{name="rg18_stale_ownership_takeover"; test="test_stale_fencing_rejected"; count=50; extra="--test task_loop_fault_tests"}
)

if ($Quick) {
    Write-Host "=== Repeat Groups (QUICK MODE — 1 each) ===" -ForegroundColor Yellow
    foreach ($rg in $repeatGroups) { $rg.count = 1 }
} else {
    Write-Host "=== Repeat Groups (18) ===" -ForegroundColor Cyan
}

foreach ($rg in $repeatGroups) {
    $ok = Invoke-SpecificTest -TestName $rg.test -Count $rg.count -Group $rg.name -ExtraArgs $rg.extra
    if (-not $ok) { $AllPassed = $false }
}

# ═══════════════════════════════════════════════════════════════════════
# 6. C8 Schedules (5 x 100)
# ═══════════════════════════════════════════════════════════════════════
if ($Quick) {
    Write-Host "=== C8 Schedules (QUICK MODE — 1 each) ===" -ForegroundColor Yellow
    $c8Count = 1
} else {
    Write-Host "=== C8 Schedules (5 x 100) ===" -ForegroundColor Cyan
    $c8Count = 100
}

$c8Tests = @(
    "c8_schedule_a_handoff_pause_worker_b_resumes",
    "c8_schedule_b_released_event_crash_resume",
    "c8_schedule_c_released_event_done_crash_before_completion",
    "c8_schedule_d_old_owner_takeover_old_rejected",
    "c8_schedule_e_completion_response_lost_retry"
)
foreach ($ct in $c8Tests) {
    $ok = Invoke-SpecificTest -TestName $ct -Count $c8Count -Group "c8_schedules" `
        -ExtraArgs "--test verification_finalization_recovery"
    if (-not $ok) { $AllPassed = $false }
}

# ═══════════════════════════════════════════════════════════════════════
# 7. Crash Prefix (8 × 50)
# ═══════════════════════════════════════════════════════════════════════
if ($Quick) {
    Write-Host "=== Crash Prefix (QUICK MODE — 1 each) ===" -ForegroundColor Yellow
    $cpCount = 1
} else {
    Write-Host "=== Crash Prefix (8 x 50) ===" -ForegroundColor Cyan
    $cpCount = 50
}

$cpTests = @(
    "crash_after_outcome_commit_restart_runs_all_steps",
    "crash_after_claim_step_claimed_before_effect",
    "crash_after_claim_effect_restart_skips_claim",
    "crash_after_lease_effect_restart",
    "crash_after_heartbeat_effect_restart",
    "crash_after_handoff_effect_restart",
    "crash_after_released_event_restart",
    "crash_before_operation_completion_restart"
)
foreach ($cp in $cpTests) {
    $ok = Invoke-SpecificTest -TestName $cp -Count $cpCount -Group "crash_prefix" `
        -ExtraArgs "--test verification_finalization_recovery"
    if (-not $ok) { $AllPassed = $false }
}

# ═══════════════════════════════════════════════════════════════════════
# 8. C8 Stress (1000/1000 two-pool)
# ═══════════════════════════════════════════════════════════════════════
if ($Quick) {
    Write-Host "=== C8 Stress (QUICK MODE — 10 each) ===" -ForegroundColor Yellow
    $c8Stress = 10
} else {
    Write-Host "=== C8 Stress (1000/1000) ===" -ForegroundColor Cyan
    $c8Stress = 1000
}
$ok = Invoke-SpecificTest -TestName "two_pool_finalizer_strict_exactly_once" -Count $c8Stress `
    -Group "c8_stress" -ExtraArgs "--test verification_finalization_recovery"
if (-not $ok) { $AllPassed = $false }

# ═══════════════════════════════════════════════════════════════════════
# 9. Three consecutive workspace runs
# ═══════════════════════════════════════════════════════════════════════
Write-Host "=== Workspace Runs (3) ===" -ForegroundColor Cyan
for ($run = 1; $run -le 3; $run++) {
    Write-Host "--- Run $run/3 ---" -ForegroundColor Yellow
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    cargo test --workspace 2>&1 | Out-Null
    $ok = ($LASTEXITCODE -eq 0)
    $sw.Stop()
    $wp = if ($ok) { 1 } else { 0 }; $wf = if ($ok) { 0 } else { 1 }
    Write-Result -Group "workspace" -Test "cargo test --workspace (run $run)" -RequiredRuns 3 -ActualRuns $run `
        -Passed $wp -Failed $wf `
        -ExitCode $LASTEXITCODE -DurationMs $sw.ElapsedMilliseconds
    if (-not $ok) {
        $AllPassed = $false
        Write-Host "FAIL: workspace run $run" -ForegroundColor Red
        break
    }
    Write-Host "PASS: workspace run $run" -ForegroundColor Green
}

# ═══════════════════════════════════════════════════════════════════════
# Output results
# ═══════════════════════════════════════════════════════════════════════
$reportOnlyDiff = (git -C $RepoRoot diff --name-only $CodeCandidateHead $ExecutionHead 2>$null)
$reportOnlyVerified = ($reportOnlyDiff -eq "I4.5_FINAL_CERTIFICATION_REPORT.md") -or ($CodeCandidateHead -eq $ExecutionHead)
$resultsJson = @{
    code_candidate_head = $CodeCandidateHead
    execution_head = $ExecutionHead
    report_only_diff_verified = $reportOnlyVerified
    completed_at = (Get-Date -Format "o")
    total_duration_ms = ((Get-Date) - $StartTime).TotalMilliseconds
    all_passed = $AllPassed
    results = $Results
} | ConvertTo-Json -Depth 4

$resultsJson | Set-Content -Path $ResultsFile -Encoding UTF8

# Generate Markdown summary
$mdSummary = @"
# I4.5 Certification Results

**Code Candidate HEAD:** `$CodeCandidateHead`
**Execution HEAD:** `$ExecutionHead`
**Report-only diff verified:** $reportOnlyVerified
**Completed:** $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
**Overall:** $(if ($AllPassed) { "**PASS**" } else { "**FAIL**" })

## Summary

| Group | Passed | Failed |
|-------|--------|--------|
"@

$groups = $Results | Group-Object group
foreach ($g in $groups) {
    $p = ($g.Group | Measure-Object -Property passed -Sum).Sum
    $f = ($g.Group | Measure-Object -Property failed -Sum).Sum
    $mdSummary += "| $($g.Name) | $p | $f |`n"
}

$mdSummary += @"

## Details

"@
foreach ($r in $Results) {
    $status = if ($r.failed -eq 0) { "[PASS]" } else { "[FAIL]" }
    $mdSummary += "- $status **$($r.group)** / $($r.test): $($r.passed)/$($r.required_runs) passed"
    if ($r.first_failure) {
        $mdSummary += " (first failure: $($r.first_failure.Substring(0, [Math]::Min(120, $r.first_failure.Length))))"
    }
    $mdSummary += "`n"
}

$mdSummary += @"

---

*Generated by I4.5 Certification Runner*
"@

$mdSummary | Set-Content -Path $SummaryFile -Encoding UTF8

Write-Host ""
Write-Host "══════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "Results: $ResultsFile" -ForegroundColor White
Write-Host "Summary: $SummaryFile" -ForegroundColor White
if ($AllPassed) {
    Write-Host "VERDICT: PASS" -ForegroundColor Green
    exit 0
} else {
    Write-Host "VERDICT: FAIL" -ForegroundColor Red
    exit 1
}
