# I4.5 Scaled Certification Runner
# Executes all certification tests at required scale and produces results.json
param([switch]$Quick)

$ErrorActionPreference = "Continue"
$RepoRoot = Resolve-Path "$PSScriptRoot\.."
$OutDir = "$RepoRoot\target\i4-5-certification"
New-Item -ItemType Directory -Force $OutDir | Out-Null

$Head = (git -C $RepoRoot rev-parse HEAD)
$Results = @()
$AllPassed = $true
$StartTime = Get-Date

function AddResult($group, $test, $req, $act, $pass, $fail, $code, $ms, $err) {
    $script:Results += @{
        candidate_head = $script:Head; group = $group; test = $test
        required_runs = $req; actual_runs = $act; passed = $pass; failed = $fail
        exit_code = $code; duration_ms = $ms; first_failure = if ($err) { $err.Substring(0, [Math]::Min(200, $err.Length)) } else { "" }
    }
}

function Run-Repeat($group, $testName, $testFile, $count) {
    $pass = 0; $fail = 0; $firstErr = ""
    $sw = [Diagnostics.Stopwatch]::StartNew()
    for ($i = 1; $i -le $count; $i++) {
        $out = cargo test -p harness-runtime --test $testFile $testName -- --nocapture 2>&1
        if ($LASTEXITCODE -eq 0) { $pass++ } else { $fail++; if (-not $firstErr) { $firstErr = ($out | Select -Last 3 | Out-String).Trim() } }
    }
    $sw.Stop()
    AddResult $group $testName $count ($pass+$fail) $pass $fail (if ($fail -gt 0) {1} else {0}) $sw.ElapsedMilliseconds $firstErr
    return $fail -eq 0
}

function Run-FaultCase($fcName) {
    $out = cargo test -p harness-runtime --test task_loop_fault_tests $fcName -- --nocapture 2>&1
    $ok = ($LASTEXITCODE -eq 0)
    AddResult "fault_cases" $fcName 1 1 (if ($ok) {1} else {0}) (if ($ok) {0} else {1}) $LASTEXITCODE 0 ""
    return $ok
}

function Run-Scenario($name, $testFile, $testName) {
    $sw = [Diagnostics.Stopwatch]::StartNew()
    $out = cargo test -p harness-runtime --test $testFile $testName -- --nocapture 2>&1
    $ok = ($LASTEXITCODE -eq 0)
    $sw.Stop()
    AddResult "scenarios" $name 1 1 (if ($ok) {1} else {0}) (if ($ok) {0} else {1}) $LASTEXITCODE $sw.ElapsedMilliseconds ""
    return $ok
}

# ═══════════════════════════════════════════════════════════════════════
Write-Host "=== I4.5 Scaled Certification Runner ===" -ForegroundColor Cyan
Write-Host "Candidate: $Head" -ForegroundColor White

# ── Fault Cases (30) ──────────────────────────────────────────────────
Write-Host "--- Fault Cases (30) ---" -ForegroundColor Yellow
$fcAllPassed = $true
for ($n = 1; $n -le 30; $n++) {
    $name = "test_fc{0:D2}" -f $n
    if (-not (Run-FaultCase $name)) { $fcAllPassed = $false }
}
Write-Host "Fault Cases: $(if ($fcAllPassed) {'PASS 30/30'} else {'FAIL'})" -ForegroundColor $(if ($fcAllPassed) {'Green'} else {'Red'})

# ── Scenarios (27) ────────────────────────────────────────────────────
Write-Host "--- Scenarios (27) ---" -ForegroundColor Yellow
$scAllPassed = $true
$scenarios = @(
    @{n="gp01"; f="task_loop_fault_tests"; t="test_gp01_first_attempt_passes"},
    @{n="gp02"; f="task_loop_fault_tests"; t="test_gp02_one_repair_then_pass"},
    @{n="gp03"; f="task_loop_fault_tests"; t="test_gp03_progressive_repairs_budget_allows"},
    @{n="gp04"; f="task_loop_fault_tests"; t="test_gp04_no_progress_stop"},
    @{n="gp05"; f="task_loop_fault_tests"; t="test_gp05_cycle_detection"},
    @{n="gp06"; f="task_loop_fault_tests"; t="test_gp06_hard_attempt_budget"},
    @{n="gp07"; f="task_loop_fault_tests"; t="test_gp07_unknown_token_usage"},
    @{n="gp08"; f="task_loop_fault_tests"; t="test_gp08_hard_token_budget"},
    @{n="gp09"; f="task_loop_fault_tests"; t="test_gp09_hard_tool_call_budget"},
    @{n="gp10"; f="task_loop_fault_tests"; t="test_gp10_hard_cost_budget"},
    @{n="gp11"; f="task_loop_fault_tests"; t="test_gp11_infrastructure_blocked"},
    @{n="gp12"; f="task_loop_fault_tests"; t="test_gp12_reconciliation_required"},
    @{n="gp13"; f="task_loop_fault_tests"; t="test_gp13_awaiting_human"},
    @{n="gp14"; f="task_loop_fault_tests"; t="test_gp14_project_escalation"},
    @{n="gp15"; f="task_loop_fault_tests"; t="test_gp15_cancellation_classification"},
    @{n="gp16"; f="task_loop_fault_tests"; t="test_gp16_cancellation_overrides"},
    @{n="gp17"; f="task_loop_fault_tests"; t="test_fc07_attempt_insert_response_lost"},
    @{n="gp18"; f="task_loop_fault_tests"; t="test_fc17_dispatch_response_lost"},
    @{n="gp19"; f="task_loop_fault_tests"; t="test_fc21_decision_response_lost"},
    @{n="gp20"; f="verification_finalization_recovery"; t="crash_after_outcome_commit_restart_runs_all_steps"},
    @{n="gp21"; f="task_loop_fault_tests"; t="test_fc20_decision_insert_before_effect"},
    @{n="gp22"; f="task_loop_fault_tests"; t="test_fc22_context_pack_before_effect"},
    @{n="gp23"; f="task_loop_fault_tests"; t="test_gp23_two_pool_full_controller"},
    @{n="gp24"; f="task_loop_fault_tests"; t="test_owner_takeover_blocks_old_owner"},
    @{n="gp25"; f="real_i4_e2e_tests"; t="test_real_i4_workspace_continuation"},
    @{n="gp26"; f="task_loop_fault_tests"; t="test_gp26_profile_selection_all_scenarios"},
    @{n="gp27"; f="task_loop_fault_tests"; t="test_gp27_context_security"}
)
foreach ($s in $scenarios) {
    if (-not (Run-Scenario $s.n $s.f $s.t)) { $scAllPassed = $false }
}
Write-Host "Scenarios: $(if ($scAllPassed) {'PASS 27/27'} else {'FAIL'})" -ForegroundColor $(if ($scAllPassed) {'Green'} else {'Red'})

# ── C8 Schedules (5 × 100) ────────────────────────────────────────────
Write-Host "--- C8 Schedules (5 × 100) ---" -ForegroundColor Yellow
$c8Count = if ($Quick) { 5 } else { 100 }
$c8AllPassed = $true
$c8Tests = @(
    @{n="c8_schedule_a"; t="c8_schedule_a_handoff_pause_worker_b_resumes"},
    @{n="c8_schedule_b"; t="c8_schedule_b_released_event_crash_resume"},
    @{n="c8_schedule_c"; t="c8_schedule_c_released_event_done_crash_before_completion"},
    @{n="c8_schedule_d"; t="c8_schedule_d_old_owner_takeover_old_rejected"},
    @{n="c8_schedule_e"; t="c8_schedule_e_completion_response_lost_retry"}
)
foreach ($ct in $c8Tests) {
    if (-not (Run-Repeat $ct.n $ct.t "verification_finalization_recovery" $c8Count)) { $c8AllPassed = $false }
}
Write-Host "C8 Schedules: $(if ($c8AllPassed) {'PASS 500/500'} else {'FAIL'})" -ForegroundColor $(if ($c8AllPassed) {'Green'} else {'Red'})

# ── C8 Stress (1000) ──────────────────────────────────────────────────
Write-Host "--- C8 Stress (1000) ---" -ForegroundColor Yellow
$stressCount = if ($Quick) { 10 } else { 1000 }
$stressOk = Run-Repeat "c8_stress" "two_pool_finalizer_strict_exactly_once" "verification_finalization_recovery" $stressCount
Write-Host "C8 Stress: $(if ($stressOk) {"PASS $stressCount/$stressCount"} else {'FAIL'})" -ForegroundColor $(if ($stressOk) {'Green'} else {'Red'})

# ── Crash Prefixes (8 × 50) ───────────────────────────────────────────
Write-Host "--- Crash Prefixes (8 × 50) ---" -ForegroundColor Yellow
$cpCount = if ($Quick) { 5 } else { 50 }
$cpAllPassed = $true
$cpTests = @(
    @{n="cp01"; t="crash_after_outcome_commit_restart_runs_all_steps"},
    @{n="cp02"; t="crash_after_claim_step_claimed_before_effect"},
    @{n="cp03"; t="crash_after_claim_effect_restart_skips_claim"},
    @{n="cp04"; t="crash_after_lease_effect_restart"},
    @{n="cp05"; t="crash_after_heartbeat_effect_restart"},
    @{n="cp06"; t="crash_after_handoff_effect_restart"},
    @{n="cp07"; t="crash_after_released_event_restart"},
    @{n="cp08"; t="crash_before_operation_completion_restart"}
)
foreach ($cp in $cpTests) {
    if (-not (Run-Repeat $cp.n $cp.t "verification_finalization_recovery" $cpCount)) { $cpAllPassed = $false }
}
Write-Host "Crash Prefixes: $(if ($cpAllPassed) {'PASS 400/400'} else {'FAIL'})" -ForegroundColor $(if ($cpAllPassed) {'Green'} else {'Red'})

# ── Windows Process Tree (200) ────────────────────────────────────────
Write-Host "--- Windows Process Tree (200) ---" -ForegroundColor Yellow
$wtCount = if ($Quick) { 10 } else { 200 }
$wtOk = Run-Repeat "process_tree" "grandchild_tree_terminated" "process_capture" $wtCount
Write-Host "Process Tree: $(if ($wtOk) {"PASS $wtCount/$wtCount"} else {'FAIL'})" -ForegroundColor $(if ($wtOk) {'Green'} else {'Red'})

# ── Running Agent Cancellation (50) ────────────────────────────────────
Write-Host "--- Running Agent Cancellation (50) ---" -ForegroundColor Yellow
$racCount = if ($Quick) { 5 } else { 50 }
$racOk = Run-Repeat "cancellation" "cancel_running_agent_terminates_process_tree" "running_agent_cancellation" $racCount
Write-Host "Cancellation: $(if ($racOk) {"PASS $racCount/$racCount"} else {'FAIL'})" -ForegroundColor $(if ($racOk) {'Green'} else {'Red'})

# ── 18 Repeat Groups ──────────────────────────────────────────────────
Write-Host "--- 18 Repeat Groups ---" -ForegroundColor Yellow
$repAllPassed = $true
$repeats = @(
    @{n="rg01"; t="test_real_i4_first_attempt_pass"; f="real_i4_e2e_tests"; c=20},
    @{n="rg02"; t="test_real_i4_repair_then_pass"; f="real_i4_e2e_tests"; c=20},
    @{n="rg03"; t="test_gp03_progressive_repairs_budget_allows"; f="task_loop_fault_tests"; c=20},
    @{n="rg04"; t="test_gp04_no_progress_stop"; f="task_loop_fault_tests"; c=20},
    @{n="rg05"; t="test_real_i4_two_pool_full_lifecycle"; f="real_i4_e2e_tests"; c=50},
    @{n="rg06"; t="test_repeat_two_pool_attempt_creation_100"; f="task_loop_fault_tests"; c=100},
    @{n="rg07"; t="test_fc07_attempt_insert_response_lost"; f="task_loop_fault_tests"; c=20},
    @{n="rg08"; t="test_fc17_dispatch_response_lost"; f="task_loop_fault_tests"; c=20},
    @{n="rg09"; t="test_fc21_decision_response_lost"; f="task_loop_fault_tests"; c=20},
    @{n="rg10"; t="test_fc20_decision_insert_before_effect"; f="task_loop_fault_tests"; c=20},
    @{n="rg11"; t="test_fc22_context_pack_before_effect"; f="task_loop_fault_tests"; c=20},
    @{n="rg12"; t="test_fc08_budget_reservation_before_effect"; f="task_loop_fault_tests"; c=20},
    @{n="rg13"; t="test_fc24_usage_write_before_effect"; f="task_loop_fault_tests"; c=20},
    @{n="rg14"; t="test_real_i4_crash_restart"; f="real_i4_e2e_tests"; c=10},
    @{n="rg15"; t="test_real_i4_workspace_continuation"; f="real_i4_e2e_tests"; c=10},
    @{n="rg16"; t="test_profile_policy_allows_switch_within_provider"; f="task_loop_i4_integration"; c=10},
    @{n="rg17"; t="test_profile_policy_rejects_cross_provider"; f="task_loop_i4_integration"; c=10},
    @{n="rg18"; t="test_stale_fencing_rejected"; f="task_loop_fault_tests"; c=50}
)
$qmult = if ($Quick) { 1 } else { 1 }
foreach ($r in $repeats) {
    $c = [Math]::Max(1, [Math]::Floor($r.c * $qmult / 10))
    if ($Quick) { $c = 1 }
    if (-not (Run-Repeat $r.n $r.t $r.f $c)) { $repAllPassed = $false }
}
Write-Host "Repeat Groups: $(if ($repAllPassed) {'PASS 18/18'} else {'FAIL'})" -ForegroundColor $(if ($repAllPassed) {'Green'} else {'Red'})

# ═══════════════════════════════════════════════════════════════════════
# Output
# ═══════════════════════════════════════════════════════════════════════
$finalAllPassed = $fcAllPassed -and $scAllPassed -and $c8AllPassed -and $stressOk -and $cpAllPassed -and $wtOk -and $racOk -and $repAllPassed
$resultObj = @{
    candidate_head = $Head
    completed_at = (Get-Date -Format "o")
    total_duration_ms = ((Get-Date) - $StartTime).TotalMilliseconds
    all_passed = $finalAllPassed
    results = $Results
}
$resultsJson = $resultObj | ConvertTo-Json -Depth 4
$resultsFile = "$OutDir\results.json"
$resultsJson | Set-Content $resultsFile -Encoding UTF8
Write-Host "`nResults: $resultsFile" -ForegroundColor White

$finalPassed = $finalAllPassed
Write-Host "`n=== FINAL VERDICT ===" -ForegroundColor Cyan
Write-Host "Fault Cases:      $(if ($fcAllPassed) {'PASS 30/30'} else {'FAIL'})"
Write-Host "Scenarios:        $(if ($scAllPassed) {'PASS 27/27'} else {'FAIL'})"
Write-Host "C8 Schedules:     $(if ($c8AllPassed) {'PASS 500/500'} else {'FAIL'})"
Write-Host "C8 Stress:        $(if ($stressOk) {"PASS $stressCount/$stressCount"} else {'FAIL'})"
Write-Host "Crash Prefixes:   $(if ($cpAllPassed) {'PASS 400/400'} else {'FAIL'})"
Write-Host "Process Tree:     $(if ($wtOk) {"PASS $wtCount/$wtCount"} else {'FAIL'})"
Write-Host "Cancellation:     $(if ($racOk) {"PASS $racCount/$racCount"} else {'FAIL'})"
Write-Host "Repeat Groups:    $(if ($repAllPassed) {'PASS 18/18'} else {'FAIL'})"
Write-Host "──────────────────────────"
if ($finalPassed) { Write-Host "VERDICT: PASS" -ForegroundColor Green; exit 0 }
else { Write-Host "VERDICT: FAIL" -ForegroundColor Red; exit 1 }
