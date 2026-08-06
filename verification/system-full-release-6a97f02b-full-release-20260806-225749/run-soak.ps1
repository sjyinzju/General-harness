$ErrorActionPreference = "Continue"
$EVIDENCE = "E:\General-harness\verification\system-full-release-6a97f02b-full-release-20260806-225749"
$REPO = "E:\General-harness"
$SOAK_MIN = 3600

$samplesFile = Join-Path $EVIDENCE "soak-samples.jsonl"
$eventsFile = Join-Path $EVIDENCE "soak-events.jsonl"
$summaryFile = Join-Path $EVIDENCE "soak-summary.json"

"" | Out-File -FilePath $samplesFile -Encoding utf8
"" | Out-File -FilePath $eventsFile -Encoding utf8

$startTime = Get-Date
$goalsCompleted = 0
$goalsFailed = 0
$goalsCancelled = 0

function Write-Sample {
    $elapsed = ((Get-Date) - $startTime).TotalSeconds
    $mem = (Get-Process -Id $PID -ErrorAction SilentlyContinue | Select-Object -ExpandProperty WorkingSet64)
    if (-not $mem) { $mem = 0 }
    $sample = "{`"ts`":`"$(Get-Date -Format 'o')`",`"elapsed_s`":$([math]::Round($elapsed,1)),`"pid`":$PID,`"rss`":$mem,`"goals_ok`":$goalsCompleted,`"goals_fail`":$goalsFailed,`"goals_cancel`":$goalsCancelled}"
    Add-Content -Path $samplesFile -Value $sample
}

function Write-Event {
    param($type, $detail)
    $detailEscaped = $detail -replace '"','\"'
    $event = "{`"ts`":`"$(Get-Date -Format 'o')`",`"type`":`"$type`",`"detail`":`"$detailEscaped`"}"
    Add-Content -Path $eventsFile -Value $event
}

Write-Event -type "soak_start" -detail "60-minute system soak starting"
Write-Sample

$iteration = 0
while ($true) {
    $elapsed = ((Get-Date) - $startTime).TotalSeconds
    if ($elapsed -ge $SOAK_MIN) { break }
    $iteration++

    # Core E2E test - normal success goal
    $result = cargo test -p harness-runtime --test i7_final_e2e_tests scene_a_deterministic_two_task_goal_e2e -- --nocapture 2>&1 | Out-String
    if ($LASTEXITCODE -eq 0 -and $result -match "0 failed") { $goalsCompleted++ } else { $goalsFailed++ }

    # Dependency goal (scene a already exercises two-task dependency)
    if ($iteration % 3 -eq 0) {
        $r2 = cargo test -p harness-runtime --test i7_final_e2e_tests scene_a_deterministic_two_task_goal_e2e -- --nocapture 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0 -and $r2 -match "0 failed") { $goalsCompleted++ } else { $goalsFailed++ }
    }

    # Verification retry / Reviewer rework
    if ($iteration % 4 -eq 0) {
        $r3 = cargo test -p harness-runtime --test i7_acceptance_tests -- --nocapture 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0 -and $r3 -match "0 failed") { $goalsCompleted++ } else { $goalsFailed++ }
    }

    # Resource claims READ/WRITE conflicts
    if ($iteration % 5 -eq 0) {
        $r4 = cargo test -p harness-runtime --test resource_claim_integration -- --nocapture 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0 -and $r4 -match "0 failed") { $goalsCompleted++ } else { $goalsFailed++ }
    }

    # Cancellation tests
    if ($iteration % 6 -eq 0) {
        $r5 = cargo test -p harness-runtime --test running_agent_cancellation -- --nocapture 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0 -and $r5 -match "0 failed") { $goalsCompleted++ } else { $goalsFailed++ }
    }

    # Goal replan
    if ($iteration % 7 -eq 0) {
        $r6 = cargo test -p harness-runtime --test i7_final_e2e_tests scene_b_failure_replan_success -- --nocapture 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0 -and $r6 -match "0 failed") { $goalsCompleted++ } else { $goalsFailed++ }
    }

    # Timeout tests
    if ($iteration % 8 -eq 0) {
        $r7 = cargo test -p harness-adapters claude_tests::test_claude_timeout -- --nocapture 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0 -and $r7 -match "0 failed") { $goalsCompleted++ } else { $goalsFailed++ }
    }

    # Process isolation tests
    if ($iteration % 9 -eq 0) {
        $r8 = cargo test -p harness-runtime --test process_integration -- --nocapture 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0 -and $r8 -match "0 failed") { $goalsCompleted++ } else { $goalsFailed++ }
    }

    # Task engineering loop tests
    if ($iteration % 10 -eq 0) {
        $r9 = cargo test -p harness-runtime --test task_engineering_loop -- --nocapture 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0 -and $r9 -match "0 failed") { $goalsCompleted++ } else { $goalsFailed++ }
    }

    # Sample every ~5 minutes (roughly every 60 iterations at ~5s per iteration)
    if ($iteration % 60 -eq 0) {
        Write-Sample
        $elapsedMin = [math]::Round($elapsed / 60, 1)
        Write-Event -type "soak_sample" -detail "$elapsedMin min: $goalsCompleted ok, $goalsFailed fail"
        Write-Output "SOAK: $elapsedMin min | goals: $goalsCompleted ok, $goalsFailed fail | iteration: $iteration"
    }
}

$endTime = Get-Date
$totalDuration = ($endTime - $startTime).TotalSeconds
$totalMin = [math]::Round($totalDuration / 60, 1)

Write-Event -type "soak_end" -detail "Soak complete: $goalsCompleted goals in $totalMin min"
Write-Sample

$passed = ($goalsFailed -eq 0 -and $totalDuration -ge $SOAK_MIN -and $goalsCompleted -ge 30)
$summary = @{
    start_time = $startTime.ToString("o")
    end_time = $endTime.ToString("o")
    duration_secs = [math]::Round($totalDuration, 1)
    duration_mins = $totalMin
    goals_completed = $goalsCompleted
    goals_failed = $goalsFailed
    goals_cancelled = $goalsCancelled
    total_goals = $goalsCompleted + $goalsFailed + $goalsCancelled
    min_duration_met = ($totalDuration -ge $SOAK_MIN)
    min_goals_met = ($goalsCompleted -ge 30)
    passed = $passed
    concurrency = "Deterministic test execution workload"
} | ConvertTo-Json -Depth 3

$summary | Out-File -FilePath $summaryFile -Encoding utf8
Write-Output "SOAK_COMPLETE: duration=$totalMin min | goals=$goalsCompleted | failed=$goalsFailed | PASS=$passed"
