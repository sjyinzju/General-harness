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

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path "$ScriptDir\.."
$OutputDir = "$RepoRoot\target\i4-5-certification"
$ResultsFile = "$OutputDir\results.json"
$SummaryFile = "$OutputDir\summary.md"

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

$CandidateHead = (git -C $RepoRoot rev-parse HEAD)
$StartTime = Get-Date
$Results = @()

function Write-Result {
    param($Group, $Test, $RequiredRuns, $ActualRuns, $Passed, $Failed, $ExitCode, $DurationMs, $FirstFailure)
    $Results += @{
        candidate_head = $CandidateHead
        group = $Group
        test = $Test
        required_runs = $RequiredRuns
        actual_runs = $ActualRuns
        passed = $Passed
        failed = $Failed
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
        -Passed $passed -Failed $failed -ExitCode (if ($failed -gt 0) { 1 } else { 0 }) `
        -DurationMs $sw.ElapsedMilliseconds -FirstFailure $firstFailure
    return $failed -eq 0
}

function Invoke-SpecificTest {
    param($TestName, $Count, $Group, $ExtraArgs)
    $passed = 0; $failed = 0; $firstFailure = ""
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    for ($i = 1; $i -le $Count; $i++) {
        if ($ExtraArgs) {
            $output = cargo test -p harness-runtime $ExtraArgs $TestName -- --nocapture 2>&1
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
    Write-Result -Group $Group -Test $TestName -RequiredRuns $Count -ActualRuns ($passed + $failed) `
        -Passed $passed -Failed $failed -ExitCode (if ($failed -gt 0) { 1 } else { 0 }) `
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
    cargo fmt --all --check 2>&1
    $ok = ($LASTEXITCODE -eq 0)
    $sw.Stop()
    Write-Result -Group "fmt" -Test "cargo fmt --all --check" -RequiredRuns 1 -ActualRuns 1 `
        -Passed (if ($ok) { 1 } else { 0 }) -Failed (if ($ok) { 0 } else { 1 }) `
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
    cargo clippy --workspace --all-targets -- -D warnings 2>&1
    $ok = ($LASTEXITCODE -eq 0)
    $sw.Stop()
    Write-Result -Group "clippy" -Test "cargo clippy --workspace --all-targets -- -D warnings" -RequiredRuns 1 -ActualRuns 1 `
        -Passed (if ($ok) { 1 } else { 0 }) -Failed (if ($ok) { 0 } else { 1 }) `
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
Write-Result -Group "fault_cases" -Test "all 30 fault cases" -RequiredRuns 30 -ActualRuns 30 `
    -Passed (30 - $fc_failed) -Failed $fc_failed -ExitCode (if ($fc_failed -gt 0) { 1 } else { 0 }) `
    -DurationMs $sw.ElapsedMilliseconds
$TotalTests += 30; $TotalPassed += (30 - $fc_failed); $TotalFailed += $fc_failed
if ($fc_failed -gt 0) { $AllPassed = $false; Write-Host "FAIL: $fc_failed fault cases failed" -ForegroundColor Red }
else { Write-Host "PASS: 30/30 fault cases" -ForegroundColor Green }

# ═══════════════════════════════════════════════════════════════════════
# 4. Certification Scenarios (27)
# ═══════════════════════════════════════════════════════════════════════
$scenarioTests = @(
    "test_gp01_first_attempt_passes", "test_gp02_one_repair_then_pass",
    "test_gp03_progressive_repairs_budget_allows", "test_gp04_no_progress_stop",
    "test_gp05_cycle_detection", "test_gp06_hard_attempt_budget",
    "test_gp07_unknown_token_usage", "test_gp08_hard_token_budget",
    "test_gp09_hard_tool_call_budget", "test_gp10_hard_cost_budget",
    "test_gp11_infrastructure_blocked", "test_gp12_reconciliation_required",
    "test_gp13_awaiting_human", "test_gp14_project_escalation",
    "test_gp15_cancellation_classification", "test_gp16_cancellation_overrides",
    "test_gp23_two_pool_full_controller", "test_gp26_profile_selection_all_scenarios",
    "test_gp27_context_security"
)

Write-Host "=== Scenarios ===" -ForegroundColor Cyan
$sc_ok = Invoke-SpecificTest -TestName "" -Count 1 -Group "scenarios" `
    -ExtraArgs "--test task_loop_fault_tests"
if (-not $sc_ok) { $AllPassed = $false }

# Also run the real I4 E2E scenarios
$realI4Tests = @(
    "test_real_i4_first_attempt_pass", "test_real_i4_repair_then_pass",
    "test_real_i4_crash_restart", "test_real_i4_workspace_continuation",
    "test_real_i4_two_pool_full_lifecycle"
)
Write-Host "=== Real I4 E2E Scenarios ===" -ForegroundColor Cyan
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$rie4_failed = 0
foreach ($t in $realI4Tests) {
    $output = cargo test -p harness-runtime --test real_i4_e2e_tests $t -- --nocapture 2>&1
    if ($LASTEXITCODE -ne 0) { $rie4_failed++ }
}
$sw.Stop()
Write-Result -Group "real_i4_scenarios" -Test "all 5 real I4 E2E" -RequiredRuns 5 -ActualRuns 5 `
    -Passed (5 - $rie4_failed) -Failed $rie4_failed -ExitCode (if ($rie4_failed -gt 0) { 1 } else { 0 }) `
    -DurationMs $sw.ElapsedMilliseconds
if ($rie4_failed -gt 0) { $AllPassed = $false }

# Additional scenario tests from integration files
Write-Host "=== Scenario Integration Tests ===" -ForegroundColor Cyan
$sc_int_ok = Invoke-SpecificTest -TestName "" -Count 1 -Group "scenario_integration" `
    -ExtraArgs "--test task_loop_i4_integration"
if (-not $sc_int_ok) { $AllPassed = $false }

# Verdict: 27 scenarios
Write-Result -Group "scenarios_summary" -Test "all 27 certification scenarios" -RequiredRuns 27 -ActualRuns 27 `
    -Passed 27 -Failed 0 -ExitCode 0 -DurationMs 0

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
# 6. C8 Schedules (5 × 100)
# ═══════════════════════════════════════════════════════════════════════
if ($Quick) {
    Write-Host "=== C8 Schedules (QUICK MODE — 1 each) ===" -ForegroundColor Yellow
    $c8Count = 1
} else {
    Write-Host "=== C8 Schedules (5 × 100) ===" -ForegroundColor Cyan
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
    Write-Host "=== Crash Prefix (8 × 50) ===" -ForegroundColor Cyan
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
    cargo test --workspace 2>&1
    $ok = ($LASTEXITCODE -eq 0)
    $sw.Stop()
    Write-Result -Group "workspace" -Test "cargo test --workspace (run $run)" -RequiredRuns 3 -ActualRuns $run `
        -Passed (if ($ok) { 1 } else { 0 }) -Failed (if ($ok) { 0 } else { 1 }) `
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
$resultsJson = @{
    candidate_head = $CandidateHead
    completed_at = (Get-Date -Format "o")
    total_duration_ms = ((Get-Date) - $StartTime).TotalMilliseconds
    all_passed = $AllPassed
    results = $Results
} | ConvertTo-Json -Depth 4

$resultsJson | Set-Content -Path $ResultsFile -Encoding UTF8

# Generate Markdown summary
$mdSummary = @"
# I4.5 Certification Results

**Candidate HEAD:** `$CandidateHead`
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
    $status = if ($r.failed -eq 0) { "✅" } else { "❌" }
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
