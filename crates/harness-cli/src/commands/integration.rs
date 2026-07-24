//! I5 CLI commands for Controlled Commit and Integration Queue management.
//!
//! Production path:
//!   enqueue: candidate-id → ControlledCommitService → commit → IntegrationRequest
//!   run-next: dequeue → lease → IntegrationExecutor → verify → publish
//!   recover: IntegrationRecoveryService → deep reconciliation

use harness_runtime::commit::ControlledCommitService;
use harness_runtime::db::Database;
use harness_runtime::integration::{IntegrationQueueService, IntegrationRecoveryService};
use std::path::Path;

// ── Enqueue: candidate → commit → integration ──────────────────────

/// Enqueue an integration request from a candidate.
/// This is the default production path:
///   1. Read CandidateSnapshot from DB
///   2. Read Approved Review from DB
///   3. Admission validation via ControlledCommitService
///   4. Create or recover CommitCandidate
///   5. Enqueue IntegrationRequest
pub async fn cmd_integration_enqueue(
    db: &Database,
    candidate_id: &str,
    repository_id: &str,
    target_ref: &str,
    priority: i32,
    repo_path: &Path,
) -> Result<String, String> {
    let commit_svc = ControlledCommitService::new(db.pool.clone());
    let queue_svc = IntegrationQueueService::new(db.pool.clone());

    // 1. Read CandidateSnapshot
    let _candidate = commit_svc
        .get_candidate_snapshot(&candidate_id.to_string())
        .await
        .map_err(|e| format!("candidate lookup: {e}"))?
        .ok_or_else(|| format!("Candidate not found: {candidate_id}"))?;

    // 2. Find approved review for this candidate
    let approved_review = commit_svc
        .find_approved_review_for_candidate(candidate_id)
        .await
        .map_err(|e| format!("review lookup: {e}"))?
        .ok_or_else(|| format!("No approved review found for candidate: {candidate_id}"))?;

    // 3. Admission validation
    let admission = commit_svc
        .validate_admission(&approved_review)
        .await
        .map_err(|e| format!("admission: {e}"))?;

    if !matches!(
        admission,
        harness_core::contracts::commit::CommitAdmission::Admitted
    ) {
        return Err(format!("admission blocked: {admission:?}"));
    }

    // 4. Create or recover commit
    use harness_core::contracts::commit::GitIdentity;
    let author = GitIdentity::new("Harness", "harness@localhost");
    let committer = GitIdentity::new("Harness Integration", "integration@localhost");

    let commit_outcome = commit_svc
        .create_commit(
            &approved_review,
            repository_id,
            target_ref,
            &author,
            &committer,
            &format!("Harness integration of candidate {}", candidate_id),
            repo_path,
        )
        .await
        .map_err(|e| format!("commit creation: {e}"))?;

    let cc = &commit_outcome.commit_candidate;

    // 5. Get current target head
    let target_head = git_rev_parse(repo_path, target_ref)?;

    // 6. Enqueue integration
    let integration_id = format!("i-{}", sqlx::types::Uuid::new_v4());
    let req = queue_svc
        .enqueue(
            &integration_id,
            &cc.commit_request_id,
            candidate_id,
            &approved_review.review_id,
            repository_id,
            target_ref,
            &target_head,
            priority,
        )
        .await
        .map_err(|e| format!("enqueue: {e}"))?;

    let output = serde_json::json!({
        "candidate_id": candidate_id,
        "review_id": approved_review.review_id,
        "commit_candidate_id": cc.commit_request_id,
        "commit_oid": cc.commit_oid,
        "integration_id": req.integration_id,
        "state": "queued",
        "idempotent_reuse": commit_outcome.recovered || req.integration_id != integration_id,
    });

    Ok(serde_json::to_string_pretty(&output).unwrap_or_default())
}

// ── Run Next: full integration execution ────────────────────────────

/// Run the next integration for a (repo, target_ref) scope.
/// Full production path: dequeue → acquire lease → execute → persist → cleanup.
pub async fn cmd_integration_run_next(
    db: &Database,
    repository_id: &str,
    target_ref: &str,
    repo_path: &Path,
    integration_root: &Path,
) -> Result<String, String> {
    let svc = IntegrationQueueService::new(db.pool.clone());
    let policy = harness_core::contracts::integration::IntegrationVerificationPolicy::default();

    match svc
        .run_next(
            repository_id,
            target_ref,
            repo_path,
            integration_root,
            &policy,
        )
        .await
        .map_err(|e| format!("run-next: {e}"))?
    {
        Some(outcome) => {
            let output = serde_json::json!({
                "integration_id": outcome.integration_id,
                "attempt_id": outcome.attempt_id,
                "lease_id": outcome.lease_id,
                "fencing_token": outcome.fencing_token,
                "previous_target_head": outcome.previous_target_head,
                "new_target_head": outcome.new_target_head,
                "strategy": outcome.strategy.map(|s| format!("{:?}", s)),
                "verification_status": outcome.verification_status,
                "state": format!("{:?}", outcome.state),
                "published": outcome.published,
            });
            Ok(serde_json::to_string_pretty(&output).unwrap_or_default())
        }
        None => {
            let output = serde_json::json!({
                "result": "NoWork",
                "message": format!("No queued integrations for {}/{}", repository_id, target_ref),
            });
            Ok(serde_json::to_string_pretty(&output).unwrap_or_default())
        }
    }
}

// ── Show ───────────────────────────────────────────────────────────

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

// ── List ────────────────────────────────────────────────────────────

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

// ── Cancel ──────────────────────────────────────────────────────────

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

// ── Recover: deep reconciliation ───────────────────────────────────

/// Recover stuck integrations via deep reconciliation.
pub async fn cmd_integration_recover(
    db: &Database,
    repo_path: &Path,
    integration_root: &Path,
    json: bool,
) -> Result<(), String> {
    let recovery = IntegrationRecoveryService::new(db.pool.clone());
    let outcome = recovery
        .reconcile(repo_path, integration_root)
        .await
        .map_err(|e| format!("recover: {e}"))?;

    if json {
        let output = serde_json::json!({
            "scanned": outcome.scanned,
            "requeued": outcome.requeued,
            "recovered_integrated": outcome.recovered_integrated,
            "failed_attempts": outcome.failed_attempts,
            "blocked": outcome.blocked,
            "leases_closed": outcome.leases_closed,
            "worktrees_cleaned": outcome.worktrees_cleaned,
            "processes_terminated": outcome.processes_terminated,
            "actions": outcome.actions,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&output).unwrap_or_default()
        );
    } else {
        println!("Integration Recovery Report:");
        println!("  Scanned: {}", outcome.scanned);
        println!("  Requeued: {}", outcome.requeued);
        println!("  Recovered (Integrated): {}", outcome.recovered_integrated);
        println!("  Failed attempts: {}", outcome.failed_attempts);
        println!("  Blocked: {}", outcome.blocked);
        println!("  Leases closed: {}", outcome.leases_closed);
        println!("  Worktrees cleaned: {}", outcome.worktrees_cleaned);
        println!("  Processes terminated: {}", outcome.processes_terminated);
        if !outcome.actions.is_empty() {
            println!("  Actions:");
            for a in &outcome.actions {
                println!(
                    "    {}: {} → {} ({})",
                    a.integration_id, a.from_state, a.to_state, a.reason
                );
            }
        }
    }
    Ok(())
}

// ── Helpers ─────────────────────────────────────────────────────────

fn git_rev_parse(repo_path: &Path, ref_name: &str) -> Result<String, String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", ref_name])
        .current_dir(repo_path)
        .output()
        .map_err(|e| e.to_string())?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        Err(format!(
            "git rev-parse {} failed: {}",
            ref_name,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}
