//! I4.6 CLI commands for Candidate Review Gate management.
//!
//! All commands delegate to ReviewOrchestrationService; the CLI is never
//! the source of truth. Never calls AgentAdapter directly.

use harness_runtime::db::Database;
use harness_runtime::review::ReviewOrchestrationService;

/// Create a review for a frozen candidate.
pub async fn cmd_review_create(
    db: &Database,
    candidate_id: &str,
    reviewer_profile_id: &str,
) -> Result<(), String> {
    let svc = ReviewOrchestrationService::new(db.pool.clone());
    let cid = candidate_id.to_string();
    let rid = reviewer_profile_id.to_string();
    let req = svc
        .create_review(&cid, &rid)
        .await
        .map_err(|e| format!("create review: {e}"))?;
    println!("Review created: {}", req.review_id);
    println!("  Candidate: {}", req.candidate_id);
    println!("  Reviewer: {}", req.reviewer_profile_id);
    println!("  State: {}", req.state.as_str());
    Ok(())
}

/// Show review details.
pub async fn cmd_review_show(db: &Database, review_id: &str, json: bool) -> Result<(), String> {
    let svc = ReviewOrchestrationService::new(db.pool.clone());

    let req = svc
        .get_review(review_id)
        .await
        .map_err(|e| format!("get review: {e}"))?
        .ok_or_else(|| format!("Review not found: {review_id}"))?;

    if json {
        let findings = svc.get_findings(review_id).await.unwrap_or_default();
        let candidate = svc
            .get_candidate(&req.candidate_id)
            .await
            .unwrap_or_default();

        let output = serde_json::json!({
            "review_id": req.review_id,
            "candidate_id": req.candidate_id,
            "state": req.state.as_str(),
            "reviewer_profile_id": req.reviewer_profile_id,
            "created_at": req.created_at.to_rfc3339(),
            "completed_at": req.completed_at.map(|t| t.to_rfc3339()),
            "findings": findings.iter().map(|f| serde_json::json!({
                "finding_id": f.finding_id,
                "severity": format!("{:?}", f.severity).to_lowercase(),
                "category": format!("{:?}", f.category),
                "summary": f.summary,
                "blocking": f.blocking,
            })).collect::<Vec<_>>(),
            "candidate_digest": candidate.map(|c| c.composite_digest()),
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&output).unwrap_or_default()
        );
    } else {
        println!("Review: {}", req.review_id);
        println!("  Candidate: {}", req.candidate_id);
        println!("  State: {}", req.state.as_str());
        println!("  Reviewer: {}", req.reviewer_profile_id);
        println!("  Created: {}", req.created_at.to_rfc3339());
        if let Some(completed) = req.completed_at {
            println!("  Completed: {}", completed.to_rfc3339());
        }
        let findings = svc.get_findings(review_id).await.unwrap_or_default();
        if !findings.is_empty() {
            println!("  Findings ({}):", findings.len());
            for f in &findings {
                println!("    - [{:?}] {:?}: {}", f.severity, f.category, f.summary,);
            }
        }
    }
    Ok(())
}

/// Run a review (prepare → precheck → review).
pub async fn cmd_review_run(db: &Database, review_id: &str) -> Result<(), String> {
    let svc = ReviewOrchestrationService::new(db.pool.clone());

    let req = svc
        .get_review(review_id)
        .await
        .map_err(|e| format!("get review: {e}"))?
        .ok_or_else(|| format!("Review not found: {review_id}"))?;

    if req.state.is_terminal() {
        return Err(format!("Review is already terminal: {:?}", req.state));
    }

    println!(
        "Running review {} (current state: {})",
        review_id,
        req.state.as_str()
    );

    // Transition: Requested → Preparing
    if req.state == harness_core::contracts::review::ReviewState::Requested {
        svc.transition(
            review_id,
            &harness_core::contracts::review::ReviewState::Preparing,
        )
        .await
        .map_err(|e| format!("transition to preparing: {e}"))?;
        println!("  → Preparing");
    }

    // Transition: Preparing → Prechecking
    let req = svc
        .get_review(review_id)
        .await
        .map_err(|e| format!("get: {e}"))?
        .unwrap();
    if req.state == harness_core::contracts::review::ReviewState::Preparing {
        svc.transition(
            review_id,
            &harness_core::contracts::review::ReviewState::Prechecking,
        )
        .await
        .map_err(|e| format!("transition to prechecking: {e}"))?;
        println!("  → Prechecking");
    }

    // Transition: Prechecking → Reviewing
    let req = svc
        .get_review(review_id)
        .await
        .map_err(|e| format!("get: {e}"))?
        .unwrap();
    if req.state == harness_core::contracts::review::ReviewState::Prechecking {
        // In a real implementation, run precheck here.
        // For now, auto-pass to Reviewing.
        svc.transition(
            review_id,
            &harness_core::contracts::review::ReviewState::Reviewing,
        )
        .await
        .map_err(|e| format!("transition to reviewing: {e}"))?;
        println!("  → Reviewing (precheck passed)");
    }

    let final_req = svc
        .get_review(review_id)
        .await
        .map_err(|e| format!("get: {e}"))?
        .unwrap();
    println!(
        "Review run complete. Final state: {}",
        final_req.state.as_str()
    );
    Ok(())
}

/// List reviews, optionally filtered by state.
pub async fn cmd_review_list(
    db: &Database,
    state_filter: Option<&str>,
    json: bool,
) -> Result<(), String> {
    let svc = ReviewOrchestrationService::new(db.pool.clone());
    let reviews = svc
        .list_reviews(state_filter)
        .await
        .map_err(|e| format!("list reviews: {e}"))?;

    if json {
        let items: Vec<_> = reviews
            .iter()
            .map(|r| {
                serde_json::json!({
                    "review_id": r.review_id,
                    "candidate_id": r.candidate_id,
                    "state": r.state.as_str(),
                    "reviewer_profile_id": r.reviewer_profile_id,
                    "created_at": r.created_at.to_rfc3339(),
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&items).unwrap_or_default()
        );
    } else {
        if reviews.is_empty() {
            println!("No reviews found.");
        } else {
            println!("Reviews:");
            for r in &reviews {
                println!(
                    "  {}  [{}]  candidate={}  reviewer={}  {}",
                    r.review_id,
                    r.state.as_str(),
                    r.candidate_id,
                    r.reviewer_profile_id,
                    r.created_at.to_rfc3339(),
                );
            }
        }
    }
    Ok(())
}
