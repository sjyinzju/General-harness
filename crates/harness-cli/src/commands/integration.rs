//! I5 CLI commands for Integration Queue management.

use harness_runtime::db::Database;
use harness_runtime::integration::IntegrationQueueService;

/// Enqueue an integration request.
#[allow(clippy::too_many_arguments)]
pub async fn cmd_integration_enqueue(
    db: &Database,
    candidate_id: &str,
    review_id: &str,
    commit_request_id: &str,
    repository_id: &str,
    target_ref: &str,
    expected_target_head: &str,
    priority: i32,
) -> Result<(), String> {
    let svc = IntegrationQueueService::new(db.pool.clone());
    let integration_id = format!("i-{}", sqlx::types::Uuid::new_v4());

    let req = svc
        .enqueue(
            &integration_id,
            commit_request_id,
            candidate_id,
            review_id,
            repository_id,
            target_ref,
            expected_target_head,
            priority,
        )
        .await
        .map_err(|e| format!("enqueue: {e}"))?;

    println!("Integration enqueued: {}", req.integration_id);
    println!("  Candidate: {}", req.candidate_id);
    println!("  Repository: {}", req.repository_id);
    println!("  Target: {}", req.target_ref);
    println!("  Priority: {}", req.priority);
    Ok(())
}

/// Dequeue and show the next integration for a (repo, target_ref).
pub async fn cmd_integration_run_next(
    db: &Database,
    repository_id: &str,
    target_ref: &str,
) -> Result<(), String> {
    let svc = IntegrationQueueService::new(db.pool.clone());

    match svc
        .dequeue(repository_id, target_ref)
        .await
        .map_err(|e| format!("dequeue: {e}"))?
    {
        Some(req) => {
            println!("Dequeued: {}", req.integration_id);
            println!("  Commit request: {}", req.commit_request_id);
            println!("  Candidate: {}", req.candidate_id);
            println!("  Priority: {}", req.priority);
            println!("  Created: {}", req.created_at.to_rfc3339());
        }
        None => {
            println!(
                "No queued integrations for {}/{}",
                repository_id, target_ref
            );
        }
    }
    Ok(())
}

/// Show an integration by ID.
pub async fn cmd_integration_show(
    db: &Database,
    integration_id: &str,
    json: bool,
) -> Result<(), String> {
    let svc = IntegrationQueueService::new(db.pool.clone());

    let req = svc
        .get(&integration_id.to_string())
        .await
        .map_err(|e| format!("get: {e}"))?
        .ok_or_else(|| format!("Integration not found: {integration_id}"))?;

    if json {
        let output = serde_json::json!({
            "integration_id": req.integration_id,
            "candidate_id": req.candidate_id,
            "review_id": req.review_id,
            "commit_request_id": req.commit_request_id,
            "repository_id": req.repository_id,
            "target_ref": req.target_ref,
            "expected_target_head": req.expected_target_head,
            "priority": req.priority,
            "created_at": req.created_at.to_rfc3339(),
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&output).unwrap_or_default()
        );
    } else {
        println!("Integration: {}", req.integration_id);
        println!("  Commit request: {}", req.commit_request_id);
        println!("  Candidate: {}", req.candidate_id);
        println!("  Review: {}", req.review_id);
        println!("  Repository: {}", req.repository_id);
        println!("  Target ref: {}", req.target_ref);
        println!("  Expected head: {}", req.expected_target_head);
        println!("  Priority: {}", req.priority);
        println!("  Created: {}", req.created_at.to_rfc3339());
    }
    Ok(())
}

/// List all integration requests.
pub async fn cmd_integration_list(db: &Database, json: bool) -> Result<(), String> {
    let svc = IntegrationQueueService::new(db.pool.clone());
    let items = svc.list_all().await.map_err(|e| format!("list: {e}"))?;

    if json {
        let output: Vec<_> = items
            .iter()
            .map(|r| {
                serde_json::json!({
                    "integration_id": r.integration_id,
                    "candidate_id": r.candidate_id,
                    "repository_id": r.repository_id,
                    "target_ref": r.target_ref,
                    "priority": r.priority,
                    "created_at": r.created_at.to_rfc3339(),
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&output).unwrap_or_default()
        );
    } else {
        if items.is_empty() {
            println!("No integration requests found.");
        } else {
            println!("Integration requests:");
            for r in &items {
                println!(
                    "  {}  repo={}  target={}  priority={}  {}",
                    r.integration_id,
                    r.repository_id,
                    r.target_ref,
                    r.priority,
                    r.created_at.to_rfc3339(),
                );
            }
        }
    }
    Ok(())
}

/// Cancel an integration.
pub async fn cmd_integration_cancel(db: &Database, integration_id: &str) -> Result<(), String> {
    let svc = IntegrationQueueService::new(db.pool.clone());
    match svc
        .cancel(&integration_id.to_string())
        .await
        .map_err(|e| format!("cancel: {e}"))?
    {
        true => println!("Cancelled: {integration_id}"),
        false => println!("Could not cancel {integration_id} (already terminal?)"),
    }
    Ok(())
}

/// Recover stuck integrations (in-progress states that appear abandoned).
pub async fn cmd_integration_recover(db: &Database) -> Result<(), String> {
    let svc = IntegrationQueueService::new(db.pool.clone());
    let all = svc.list_all().await.map_err(|e| format!("list: {e}"))?;

    let mut recovered = 0;
    for req in &all {
        // List each item — recovery moves stuck items back to queued
        let _ = svc.get(&req.integration_id).await;
        // For now, just list recoverable items
        println!(
            "  {} (use 'cancel' or re-enqueue for stuck items)",
            req.integration_id
        );
        recovered += 1;
    }
    println!("Listed {} integration requests", recovered);
    Ok(())
}
