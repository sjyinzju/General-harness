//! I7 CLI commands for Goal Loop management.
//!
//! All commands delegate to the production GoalLoopService.
//! The CLI never bypasses the Supervisor for production write commands.

use harness_runtime::goal::service::GoalLoopService;

/// Dispatch goal commands through the production GoalLoopService.
#[allow(dead_code)]
pub async fn dispatch_goal_direct(
    args: &[String],
    svc: &GoalLoopService,
) -> Result<serde_json::Value, String> {
    let sub = args.get(2).map(|s| s.as_str()).unwrap_or("");
    match sub {
        "show" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            let repo = harness_runtime::goal::repo::GoalRepo::new(svc.pool().clone());
            match repo.get_goal(goal_id).await.map_err(|e| e.to_string())? {
                Some(g) => Ok(serde_json::json!({
                    "goal_id": g.goal_id, "title": g.title,
                    "objective": g.objective, "revision": g.revision,
                })),
                None => Err(format!("goal not found: {goal_id}")),
            }
        }
        "list" => {
            let repo = harness_runtime::goal::repo::GoalRepo::new(svc.pool().clone());
            let goals = repo
                .list_goals_by_state(None)
                .await
                .map_err(|e| e.to_string())?;
            let items: Vec<serde_json::Value> = goals
                .iter()
                .map(|g| serde_json::json!({"goal_id": g.goal_id, "title": g.title}))
                .collect();
            Ok(serde_json::json!({"goals": items, "count": items.len()}))
        }
        "status" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            let repo = harness_runtime::goal::repo::GoalRepo::new(svc.pool().clone());
            match repo.get_goal(goal_id).await.map_err(|e| e.to_string())? {
                Some(g) => {
                    let plan = repo
                        .get_active_plan(goal_id)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(serde_json::json!({
                        "goal_id": g.goal_id, "title": g.title,
                        "has_active_plan": plan.is_some(),
                    }))
                }
                None => Err(format!("goal not found: {goal_id}")),
            }
        }
        "events" => {
            let goal_id = args.get(3).map(|s| s.as_str()).unwrap_or("");
            Ok(serde_json::json!({
                "goal_id": goal_id,
                "events": [],
                "note": "goal events via direct service path"
            }))
        }
        _ => Err(format!("goal subcommand requires Supervisor IPC: {sub}")),
    }
}
