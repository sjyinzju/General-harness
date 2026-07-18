//! I4.5 CLI commands for Task Engineering Loop management.
//!
//! All commands delegate to runtime services; the CLI is never the
//! source of truth. Never calls AgentAdapter directly, never spawns
//! processes, never contains business logic.

use harness_runtime::db::Database;
use harness_runtime::task_loop::*;

/// Print loop status in human-readable format.
pub async fn cmd_status(db: &Database, loop_id: &str) -> Result<(), String> {
    let svc = TaskEngineeringLoopService::new(db.pool.clone());
    match svc.inspect_loop(loop_id).await? {
        Some(info) => {
            println!("Loop: {}", info.loop_id);
            println!("  Task: {}", info.task_id);
            println!("  Lifecycle: {}", info.lifecycle.as_str());
            println!(
                "  Attempts: {} (current ordinal: {})",
                info.attempt_count, info.current_ordinal
            );
            println!("  No-progress streak: {}", info.no_progress_streak);
            println!("  Same-failure streak: {}", info.same_failure_streak);
            println!("  Profile switches: {}", info.profile_switch_count);
            println!("  Owner: {:?}", info.owner_id);
            println!("  Fencing token: {}", info.fencing_token);
            if let Some(a) = &info.active_attempt {
                println!("  Active Attempt:");
                println!("    ID: {}", a.attempt_id);
                println!("    Ordinal: {}", a.ordinal);
                println!("    Execution: {:?}", a.execution_id);
                println!("    Lifecycle: {}", a.lifecycle.as_str());
                println!("    Outcome: {:?}", a.outcome_kind);
            }
            let usage = &info.usage_summary;
            println!("  Usage:");
            println!("    Input tokens: {:?}", usage.total_input_tokens);
            println!("    Output tokens: {:?}", usage.total_output_tokens);
            println!("    Tool calls: {:?}", usage.total_tool_calls);
            println!("    Wall time: {:?}ms", usage.total_wall_time_ms);
            println!("    Cost micros: {:?}", usage.total_estimated_cost_micros);
            if let Some(err) = &info.last_error {
                println!("  Last error: {err}");
            }
            Ok(())
        }
        None => Err(format!("Loop not found: {loop_id}")),
    }
}

/// Print loop status as JSON.
pub async fn cmd_inspect_json(db: &Database, loop_id: &str) -> Result<(), String> {
    let svc = TaskEngineeringLoopService::new(db.pool.clone());
    match svc.inspect_loop(loop_id).await? {
        Some(info) => {
            let json = serde_json::json!({
                "loop_id": info.loop_id,
                "task_id": info.task_id,
                "lifecycle": info.lifecycle.as_str(),
                "attempt_count": info.attempt_count,
                "current_ordinal": info.current_ordinal,
                "no_progress_streak": info.no_progress_streak,
                "same_failure_streak": info.same_failure_streak,
                "profile_switch_count": info.profile_switch_count,
                "owner_id": info.owner_id,
                "fencing_token": info.fencing_token,
                "active_attempt": info.active_attempt.as_ref().map(|a| serde_json::json!({
                    "attempt_id": a.attempt_id,
                    "ordinal": a.ordinal,
                    "execution_id": a.execution_id,
                    "lifecycle": a.lifecycle.as_str(),
                    "outcome_kind": a.outcome_kind,
                })),
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&json).unwrap_or_default()
            );
            Ok(())
        }
        None => Err(format!("Loop not found: {loop_id}")),
    }
}

/// Start a new task engineering loop.
pub async fn cmd_start(
    db: &Database,
    project_id: &str,
    task_id: &str,
    owner_id: &str,
    policy_json: &str,
) -> Result<(), String> {
    let svc = TaskEngineeringLoopService::new(db.pool.clone());
    let policy_fp = fingerprint_hex(policy_json);
    let ikey = format!("tl-start-{task_id}");
    let req = CreateLoopRequest {
        project_id: project_id.to_string(),
        task_id: task_id.to_string(),
        policy_json: policy_json.to_string(),
        policy_fingerprint: policy_fp,
        idempotency_key: ikey.clone(),
        request_hash: ikey,
        owner_id: owner_id.to_string(),
        lease_secs: 300,
    };
    match svc.create_loop(&req).await? {
        CreateLoopOutcome::Created { loop_id } => {
            let _ = svc.start_or_resume_loop(&loop_id, owner_id, 300).await?;
            println!("Loop created and started: {loop_id}");
            Ok(())
        }
        CreateLoopOutcome::Duplicate { loop_id } => {
            let _ = svc.start_or_resume_loop(&loop_id, owner_id, 300).await?;
            println!("Loop already exists, resumed: {loop_id}");
            Ok(())
        }
        other => Err(format!("{other:?}")),
    }
}

/// Resume an existing loop.
pub async fn cmd_resume(db: &Database, loop_id: &str, owner_id: &str) -> Result<(), String> {
    let svc = TaskEngineeringLoopService::new(db.pool.clone());
    match svc.start_or_resume_loop(loop_id, owner_id, 300).await? {
        LoopStartOutcome::Started { .. } => {
            println!("Loop started: {loop_id}");
            Ok(())
        }
        LoopStartOutcome::Resumed { lifecycle, .. } => {
            println!(
                "Loop resumed (lifecycle: {}): {loop_id}",
                lifecycle.as_str()
            );
            Ok(())
        }
        LoopStartOutcome::AlreadyTerminal { lifecycle } => {
            println!("Loop is terminal ({}): {loop_id}", lifecycle.as_str());
            Ok(())
        }
        other => Err(format!("{other:?}")),
    }
}

/// Cancel a loop.
pub async fn cmd_cancel(db: &Database, loop_id: &str, owner_id: &str) -> Result<(), String> {
    let svc = TaskEngineeringLoopService::new(db.pool.clone());
    let l = TaskLoopRepo::new(db.pool.clone())
        .load_loop(loop_id)
        .await?
        .ok_or("loop not found")?;
    match svc
        .cancel_loop(loop_id, owner_id, l.version, l.fencing_token)
        .await?
    {
        CancelLoopOutcome::Cancelled => println!("Loop cancelled: {loop_id}"),
        CancelLoopOutcome::AlreadyTerminal { lifecycle } => {
            println!("Already terminal ({}): {loop_id}", lifecycle.as_str())
        }
    }
    Ok(())
}

/// Dry-run decision without modifying state.
pub async fn cmd_dry_run_decision(db: &Database, loop_id: &str) -> Result<(), String> {
    let repo = TaskLoopRepo::new(db.pool.clone());
    let l = repo.load_loop(loop_id).await?.ok_or("loop not found")?;
    let active = repo.load_active_attempt(loop_id).await?;
    let usage = repo.sum_loop_usage(loop_id).await?;

    println!("Loop: {} (lifecycle: {})", loop_id, l.lifecycle.as_str());
    println!(
        "Attempts: {} (ordinal: {})",
        l.attempt_count, l.current_attempt_ordinal
    );
    println!(
        "No-progress: {}, same-failure: {}",
        l.no_progress_streak, l.same_failure_streak
    );

    if let Some(a) = &active {
        println!(
            "Active attempt: {} (ordinal: {}, lifecycle: {})",
            a.attempt_id,
            a.ordinal,
            a.lifecycle.as_str()
        );
        println!("  Execution: {:?}", a.execution_id);
        println!("  Outcome kind: {:?}", a.outcome_kind);
    }

    let budget = BudgetPolicy::default();
    let check = budget.check_can_attempt(
        l.attempt_count,
        l.no_progress_streak,
        l.same_failure_streak,
        l.profile_switch_count,
        usage.total_input_tokens,
        usage.total_output_tokens,
        None,
        usage.total_tool_calls,
        usage.total_wall_time_ms,
        usage.total_estimated_cost_micros,
        true,
    );
    println!("Budget check: {check:?}");

    // Read I4 facts — use the runtime Database which has sqlx internally.
    if let Some(ref eid) = active.as_ref().and_then(|a| a.execution_id.as_ref()) {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT lifecycle FROM execution_attempts WHERE id=?")
                .bind(eid)
                .fetch_optional(&db.pool)
                .await
                .ok()
                .flatten();
        if let Some((lc,)) = row {
            println!("Execution lifecycle: {lc}");
            let input = DecisionInput {
                cancellation_requested: false,
                i4_reconciliation_required: false,
                active_process: false,
                active_scanner: false,
                ownership_fencing_ok: true,
                worktree_identity_ok: true,
                security_blocker: false,
                outcome_result: active.as_ref().and_then(|a| a.outcome_kind.clone()),
                next_action: None,
                all_required_steps_passed: lc == "completed",
                evidence_complete: lc == "completed",
                dossier_present: lc == "completed",
                dossier_fingerprint_matches: lc == "completed",
                budget_exhausted_hard: matches!(check, BudgetCheckResult::Exhausted { .. }),
                no_progress: l.no_progress_streak >= 3,
                cycle_detected: false,
                infrastructure_blocked: false,
                repairable: active
                    .as_ref()
                    .and_then(|a| a.outcome_kind.as_ref())
                    .map(|o| decision::is_default_repairable(o))
                    .unwrap_or(false),
                task_scope_insufficient: false,
                primary_failure: active.as_ref().and_then(|a| a.outcome_kind.clone()),
            };
            let decision = input.classify();
            println!(
                "Decision: {} (action: {})",
                decision.as_str(),
                decision.action()
            );
        }
    }
    Ok(())
}
