//! I5.3 Sandboxed Integration — end-to-end tests with real Git repos.

use harness_core::contracts::integration::{
    IntegrationAttempt, IntegrationState, IntegrationStrategy, IntegrationVerificationPolicy,
    VerificationCommand,
};
use harness_runtime::integration::IntegrationExecutor;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

fn git_init(repo_path: &Path) {
    Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(repo_path)
        .output()
        .unwrap();
    Command::new("git")
        .args(["config", "user.name", "T"])
        .current_dir(repo_path)
        .output()
        .unwrap();
    Command::new("git")
        .args(["config", "user.email", "t@t.com"])
        .current_dir(repo_path)
        .output()
        .unwrap();
    // Create initial commit so refs/heads/main exists
    std::fs::write(repo_path.join("f1.txt"), "base\n").unwrap();
    Command::new("git")
        .args(["add", "f1.txt"])
        .current_dir(repo_path)
        .output()
        .unwrap();
    Command::new("git")
        .args(["commit", "-m", "base"])
        .current_dir(repo_path)
        .output()
        .unwrap();
}

fn git_rev_parse(repo_path: &Path, ref_name: &str) -> Result<String, String> {
    let out = Command::new("git")
        .args(["rev-parse", ref_name])
        .current_dir(repo_path)
        .output()
        .map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

fn create_commit(repo_path: &Path, file: &str, content: &str, msg: &str) -> String {
    std::fs::write(repo_path.join(file), content).unwrap();
    Command::new("git")
        .args(["add", file])
        .current_dir(repo_path)
        .output()
        .unwrap();
    Command::new("git")
        .args(["commit", "-m", msg])
        .current_dir(repo_path)
        .output()
        .unwrap();
    git_rev_parse(repo_path, "HEAD").unwrap()
}

fn make_pool() -> sqlx::SqlitePool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        harness_runtime::db::Database::open_in_memory()
            .await
            .unwrap()
            .pool
    })
}

fn noop_policy() -> IntegrationVerificationPolicy {
    IntegrationVerificationPolicy {
        commands: vec![],
        timeout_secs: 30,
        max_output_bytes: 64_000,
        required: false,
    }
}

fn required_failing_policy() -> IntegrationVerificationPolicy {
    IntegrationVerificationPolicy {
        commands: vec![VerificationCommand {
            program: "nonexistent-xyz-12345".into(),
            args: vec![],
            working_dir: None,
        }],
        timeout_secs: 5,
        max_output_bytes: 1000,
        required: true,
    }
}

#[test]
fn test_strategy_fast_forward() {
    let dir = TempDir::new().unwrap();
    let rp = dir.path();
    git_init(rp);
    let base = git_rev_parse(rp, "HEAD").unwrap();
    assert_eq!(
        IntegrationExecutor::resolve_strategy(&base, &base),
        IntegrationStrategy::FastForward
    );
}

#[test]
fn test_strategy_cherry_pick() {
    assert_eq!(
        IntegrationExecutor::resolve_strategy("a", "b"),
        IntegrationStrategy::CherryPick
    );
}

#[test]
fn test_fast_forward_integration_success() {
    let dir = TempDir::new().unwrap();
    let rp = dir.path();
    git_init(rp);
    let base = git_rev_parse(rp, "HEAD").unwrap();
    let candidate = create_commit(rp, "f2.txt", "new\n", "candidate");
    // Reset main back to base so we can fast-forward to candidate
    Command::new("git")
        .args(["update-ref", "refs/heads/main", &base])
        .current_dir(rp)
        .output()
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let exec = IntegrationExecutor::new(make_pool(), tmp.path());

    let attempt = IntegrationAttempt {
        attempt_id: "a".into(),
        integration_id: "i".into(),
        attempt_number: 1,
        state: IntegrationState::Preparing,
        commit_oid: candidate.clone(),
        parent_oid: base.clone(),
        target_head_at_start: base.clone(),
        integration_tree_oid: None,
        integration_commit_oid: None,
        lease_id: None,
        fencing_token: None,
        started_at: None,
        completed_at: None,
        created_at: chrono::Utc::now(),
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let outcome = rt.block_on(async {
        exec.execute(
            "i",
            &attempt,
            rp,
            "refs/heads/main",
            "test-repo",
            "test-lease",
            1,
            &noop_policy(),
        )
        .await
        .unwrap()
    });
    assert!(outcome.published, "FF publish should succeed");
    assert_eq!(outcome.result.state, IntegrationState::Integrated);
    assert_eq!(git_rev_parse(rp, "refs/heads/main").unwrap(), candidate);
    exec.cleanup_worktree(rp, &exec.integration_worktree_path("i", "a"));
}

#[test]
fn test_conflict_integration() {
    let dir = TempDir::new().unwrap();
    let rp = dir.path();
    git_init(rp);
    let base = git_rev_parse(rp, "HEAD").unwrap();

    // Candidate edits f1.txt
    std::fs::write(rp.join("f1.txt"), "CANDIDATE CHANGE\n").unwrap();
    Command::new("git")
        .args(["add", "f1.txt"])
        .current_dir(rp)
        .output()
        .unwrap();
    Command::new("git")
        .args(["commit", "-m", "candidate"])
        .current_dir(rp)
        .output()
        .unwrap();
    let candidate = git_rev_parse(rp, "HEAD").unwrap();

    // Reset working tree and main back to base
    Command::new("git")
        .args(["checkout", "--", "."])
        .current_dir(rp)
        .output()
        .unwrap();
    Command::new("git")
        .args(["update-ref", "refs/heads/main", &base])
        .current_dir(rp)
        .output()
        .unwrap();
    // Target makes conflicting edit to same file
    std::fs::write(rp.join("f1.txt"), "TARGET CHANGE\n").unwrap();
    Command::new("git")
        .args(["add", "f1.txt"])
        .current_dir(rp)
        .output()
        .unwrap();
    Command::new("git")
        .args(["commit", "-m", "target"])
        .current_dir(rp)
        .output()
        .unwrap();
    let target_head = git_rev_parse(rp, "HEAD").unwrap();

    let tmp = TempDir::new().unwrap();
    let exec = IntegrationExecutor::new(make_pool(), tmp.path());

    let attempt = IntegrationAttempt {
        attempt_id: "a".into(),
        integration_id: "i".into(),
        attempt_number: 1,
        state: IntegrationState::Preparing,
        commit_oid: candidate,
        parent_oid: base,
        target_head_at_start: target_head,
        integration_tree_oid: None,
        integration_commit_oid: None,
        lease_id: None,
        fencing_token: None,
        started_at: None,
        completed_at: None,
        created_at: chrono::Utc::now(),
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let outcome = rt.block_on(async {
        exec.execute(
            "i",
            &attempt,
            rp,
            "refs/heads/main",
            "test-repo",
            "test-lease",
            1,
            &noop_policy(),
        )
        .await
        .unwrap()
    });
    assert!(!outcome.published);
    assert_eq!(outcome.result.state, IntegrationState::Conflict);
    let c = outcome.result.conflicts.unwrap();
    assert!(!c.conflicting_files.is_empty());
    exec.cleanup_worktree(rp, &exec.integration_worktree_path("i", "a"));
}

#[test]
fn test_advanced_target_no_conflict() {
    let dir = TempDir::new().unwrap();
    let rp = dir.path();
    git_init(rp);
    let base = git_rev_parse(rp, "HEAD").unwrap();

    let candidate = create_commit(rp, "f2.txt", "candidate\n", "candidate");
    // Reset to base
    Command::new("git")
        .args(["reset", "--hard", &base])
        .current_dir(rp)
        .output()
        .unwrap();
    // Target adds a DIFFERENT file
    let _target = create_commit(rp, "f3.txt", "target\n", "target");
    let target_head = git_rev_parse(rp, "HEAD").unwrap();

    let tmp = TempDir::new().unwrap();
    let exec = IntegrationExecutor::new(make_pool(), tmp.path());

    let attempt = IntegrationAttempt {
        attempt_id: "a".into(),
        integration_id: "i".into(),
        attempt_number: 1,
        state: IntegrationState::Preparing,
        commit_oid: candidate,
        parent_oid: base,
        target_head_at_start: target_head,
        integration_tree_oid: None,
        integration_commit_oid: None,
        lease_id: None,
        fencing_token: None,
        started_at: None,
        completed_at: None,
        created_at: chrono::Utc::now(),
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let outcome = rt.block_on(async {
        exec.execute(
            "i",
            &attempt,
            rp,
            "refs/heads/main",
            "test-repo",
            "test-lease",
            1,
            &noop_policy(),
        )
        .await
        .unwrap()
    });
    assert!(outcome.published);
    assert_eq!(outcome.result.state, IntegrationState::Integrated);
    exec.cleanup_worktree(rp, &exec.integration_worktree_path("i", "a"));
}

#[test]
fn test_verification_failure_blocks_publish() {
    let dir = TempDir::new().unwrap();
    let rp = dir.path();
    git_init(rp);
    let base = git_rev_parse(rp, "HEAD").unwrap();
    let candidate = create_commit(rp, "f2.txt", "ok\n", "candidate");
    // Reset main back to base
    Command::new("git")
        .args(["update-ref", "refs/heads/main", &base])
        .current_dir(rp)
        .output()
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let exec = IntegrationExecutor::new(make_pool(), tmp.path());

    let attempt = IntegrationAttempt {
        attempt_id: "a".into(),
        integration_id: "i".into(),
        attempt_number: 1,
        state: IntegrationState::Preparing,
        commit_oid: candidate,
        parent_oid: base.clone(),
        target_head_at_start: base.clone(),
        integration_tree_oid: None,
        integration_commit_oid: None,
        lease_id: None,
        fencing_token: None,
        started_at: None,
        completed_at: None,
        created_at: chrono::Utc::now(),
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let outcome = rt.block_on(async {
        exec.execute(
            "i",
            &attempt,
            rp,
            "refs/heads/main",
            "test-repo",
            "test-lease",
            1,
            &required_failing_policy(),
        )
        .await
        .unwrap()
    });

    assert!(!outcome.published);
    assert_eq!(outcome.result.state, IntegrationState::Failed);
    let head = git_rev_parse(rp, "refs/heads/main").unwrap();
    assert_eq!(head, base, "target ref must not change");
    exec.cleanup_worktree(rp, &exec.integration_worktree_path("i", "a"));
}

#[test]
fn test_cas_race_target_advanced() {
    let dir = TempDir::new().unwrap();
    let rp = dir.path();
    git_init(rp);
    let base = git_rev_parse(rp, "HEAD").unwrap();
    let candidate = create_commit(rp, "f2.txt", "ok\n", "candidate");

    let tmp = TempDir::new().unwrap();
    let exec = IntegrationExecutor::new(make_pool(), tmp.path());

    let attempt = IntegrationAttempt {
        attempt_id: "a".into(),
        integration_id: "i".into(),
        attempt_number: 1,
        state: IntegrationState::Preparing,
        commit_oid: candidate,
        parent_oid: base.clone(),
        target_head_at_start: base,
        integration_tree_oid: None,
        integration_commit_oid: None,
        lease_id: None,
        fencing_token: None,
        started_at: None,
        completed_at: None,
        created_at: chrono::Utc::now(),
    };

    // Advance target concurrently
    let _ = create_commit(rp, "f3.txt", "concurrent\n", "concurrent");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let outcome = rt.block_on(async {
        exec.execute(
            "i",
            &attempt,
            rp,
            "refs/heads/main",
            "test-repo",
            "test-lease",
            1,
            &noop_policy(),
        )
        .await
        .unwrap()
    });
    assert!(!outcome.published);
    assert_eq!(outcome.result.state, IntegrationState::Failed);
    exec.cleanup_worktree(rp, &exec.integration_worktree_path("i", "a"));
}

#[test]
fn test_integration_worktree_isolation() {
    let dir = TempDir::new().unwrap();
    let rp = dir.path();
    git_init(rp);
    let base = git_rev_parse(rp, "HEAD").unwrap();
    let candidate = create_commit(rp, "f2.txt", "ok\n", "candidate");

    let tmp = TempDir::new().unwrap();
    let exec = IntegrationExecutor::new(make_pool(), tmp.path());
    let wt_path = exec.integration_worktree_path("i", "a");
    assert!(wt_path.starts_with(tmp.path()));

    let attempt = IntegrationAttempt {
        attempt_id: "a".into(),
        integration_id: "i".into(),
        attempt_number: 1,
        state: IntegrationState::Preparing,
        commit_oid: candidate,
        parent_oid: base.clone(),
        target_head_at_start: base,
        integration_tree_oid: None,
        integration_commit_oid: None,
        lease_id: None,
        fencing_token: None,
        started_at: None,
        completed_at: None,
        created_at: chrono::Utc::now(),
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        exec.execute(
            "i",
            &attempt,
            rp,
            "refs/heads/main",
            "test-repo",
            "test-lease",
            1,
            &noop_policy(),
        )
        .await
        .unwrap()
    });
    // Cleanup after execution
    exec.cleanup_worktree(rp, &wt_path);
    assert!(
        !wt_path.exists(),
        "integration worktree should be cleaned up"
    );
}
