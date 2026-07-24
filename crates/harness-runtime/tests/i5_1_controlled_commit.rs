//! I5.1 Controlled Commit — integration tests.
//!
//! Tests: admission validation, commit creation, idempotency, recovery.

use chrono::Utc;
use harness_core::contracts::commit::{CommitAdmission, GitIdentity};
use harness_core::contracts::review::ApprovedCandidate;
use harness_runtime::commit::ControlledCommitService;
use harness_runtime::db::Database;
use sqlx::SqlitePool;
use std::process::Command;
use tempfile::TempDir;
use uuid::Uuid;

// ── Async Helpers ────────────────────────────────────────────────────

async fn setup_db() -> Database {
    let db = Database::open_in_memory().await.unwrap();
    sqlx::query("INSERT INTO projects(id,objective,lifecycle) VALUES('p1','test','active')")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('t1','p1','test','verified')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e1','t1',1,'completed')")
        .execute(&db.pool).await.unwrap();
    db
}

async fn seed_candidate(
    pool: &SqlitePool,
    candidate_id: &str,
    executor_profile_id: &str,
    base_commit: &str,
    tree_hash: &str,
    diff_digest: &str,
) {
    sqlx::query(
        "INSERT OR IGNORE INTO candidate_snapshots (candidate_id, task_id, execution_id, executor_profile_id, workspace_id, base_commit, candidate_tree_hash, diff_digest, task_spec_digest, evidence_digest, composite_digest, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))",
    )
    .bind(candidate_id).bind("t1").bind("e1").bind(executor_profile_id).bind("w1")
    .bind(base_commit).bind(tree_hash).bind(diff_digest)
    .bind("task123").bind("ev123").bind("comp123")
    .execute(pool).await.unwrap();
}

async fn seed_review(
    pool: &SqlitePool,
    review_id: &str,
    candidate_id: &str,
    reviewer_profile_id: &str,
    state: &str,
) {
    sqlx::query(
        "INSERT OR IGNORE INTO review_requests (review_id, candidate_id, reviewer_profile_id, state, idempotency_key, request_hash) VALUES (?,?,?,?,?,?)",
    )
    .bind(review_id).bind(candidate_id).bind(reviewer_profile_id).bind(state)
    .bind(format!("ik-{review_id}")).bind(format!("hash-{review_id}"))
    .execute(pool).await.unwrap();

    if state == "approved" {
        sqlx::query(
            "INSERT OR IGNORE INTO review_decisions (decision_id, review_id, candidate_id, decision, summary, candidate_digest_at_decision, decision_digest, findings_count, reviewer_output_json) VALUES (?,?,?,?,?,?,?,?,?)",
        )
        .bind(format!("dec-{review_id}")).bind(review_id).bind(candidate_id)
        .bind("approved").bind("clean").bind("comp123")
        .bind(format!("digest-{review_id}")).bind(0).bind("{}")
        .execute(pool).await.unwrap();
    }
}

fn setup_git_repo() -> (TempDir, String) {
    let dir = TempDir::new().unwrap();
    let repo_path = dir.path();
    Command::new("git")
        .args(["init"])
        .current_dir(repo_path)
        .output()
        .unwrap();
    Command::new("git")
        .args(["config", "user.name", "Test Author"])
        .current_dir(repo_path)
        .output()
        .unwrap();
    Command::new("git")
        .args(["config", "user.email", "author@test.com"])
        .current_dir(repo_path)
        .output()
        .unwrap();
    std::fs::write(repo_path.join("README.md"), "# Test\n").unwrap();
    Command::new("git")
        .args(["add", "README.md"])
        .current_dir(repo_path)
        .output()
        .unwrap();
    Command::new("git")
        .args(["commit", "-m", "initial commit"])
        .current_dir(repo_path)
        .output()
        .unwrap();
    let base_oid = String::from_utf8_lossy(
        &Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(repo_path)
            .output()
            .unwrap()
            .stdout,
    )
    .trim()
    .to_string();
    (dir, base_oid)
}

fn get_tree_oid(repo_path: &std::path::Path) -> String {
    String::from_utf8_lossy(
        &Command::new("git")
            .args(["rev-parse", "HEAD^{tree}"])
            .current_dir(repo_path)
            .output()
            .unwrap()
            .stdout,
    )
    .trim()
    .to_string()
}

// ── Admission Tests ───────────────────────────────────────────────────

#[tokio::test]
async fn test_admission_non_approved_review_blocked() {
    let db = setup_db().await;
    let candidate_id = format!("cand-{}", Uuid::new_v4());
    seed_candidate(
        &db.pool,
        &candidate_id,
        "executor1",
        "base123",
        "tree123",
        "diff123",
    )
    .await;
    let review_id = "rev-nonapproved";
    seed_review(&db.pool, review_id, &candidate_id, "reviewer1", "requested").await;

    let svc = ControlledCommitService::new(db.pool.clone());
    let approved = ApprovedCandidate {
        candidate_id: candidate_id.clone(),
        review_id: review_id.into(),
        candidate_tree_hash: "tree123".into(),
        diff_digest: "diff123".into(),
        review_decision_digest: format!("digest-{review_id}"),
        approved_at: Utc::now(),
    };
    let admission = svc.validate_admission(&approved).await.unwrap();
    assert!(
        matches!(admission, CommitAdmission::Blocked { .. }),
        "Expected Blocked, got: {admission:?}"
    );
}

#[tokio::test]
async fn test_admission_stale_candidate() {
    let db = setup_db().await;
    let candidate_id = format!("cand-{}", Uuid::new_v4());
    seed_candidate(
        &db.pool,
        &candidate_id,
        "executor1",
        "base123",
        "tree123",
        "diff123",
    )
    .await;
    let review_id = "rev-stale";
    seed_review(&db.pool, review_id, &candidate_id, "reviewer1", "approved").await;

    let svc = ControlledCommitService::new(db.pool.clone());
    let approved = ApprovedCandidate {
        candidate_id: candidate_id.clone(),
        review_id: review_id.into(),
        candidate_tree_hash: "wrong_tree".into(),
        diff_digest: "diff123".into(),
        review_decision_digest: format!("digest-{review_id}"),
        approved_at: Utc::now(),
    };
    let admission = svc.validate_admission(&approved).await.unwrap();
    assert!(
        matches!(admission, CommitAdmission::Stale { .. }),
        "Expected Stale, got: {admission:?}"
    );
}

#[tokio::test]
async fn test_admission_diff_digest_mismatch_stale() {
    let db = setup_db().await;
    let candidate_id = format!("cand-{}", Uuid::new_v4());
    seed_candidate(
        &db.pool,
        &candidate_id,
        "executor1",
        "base123",
        "tree123",
        "diff123",
    )
    .await;
    let review_id = "rev-diffm";
    seed_review(&db.pool, review_id, &candidate_id, "reviewer1", "approved").await;

    let svc = ControlledCommitService::new(db.pool.clone());
    let approved = ApprovedCandidate {
        candidate_id: candidate_id.clone(),
        review_id: review_id.into(),
        candidate_tree_hash: "tree123".into(),
        diff_digest: "wrong_diff".into(),
        review_decision_digest: format!("digest-{review_id}"),
        approved_at: Utc::now(),
    };
    let admission = svc.validate_admission(&approved).await.unwrap();
    assert!(
        matches!(admission, CommitAdmission::Stale { .. }),
        "Expected Stale, got: {admission:?}"
    );
}

#[tokio::test]
async fn test_admission_reviewer_equals_executor_blocked() {
    let db = setup_db().await;
    let candidate_id = format!("cand-{}", Uuid::new_v4());
    seed_candidate(
        &db.pool,
        &candidate_id,
        "same_profile",
        "base123",
        "tree123",
        "diff123",
    )
    .await;
    let review_id = "rev-same";
    seed_review(
        &db.pool,
        review_id,
        &candidate_id,
        "same_profile",
        "approved",
    )
    .await;

    let svc = ControlledCommitService::new(db.pool.clone());
    let approved = ApprovedCandidate {
        candidate_id: candidate_id.clone(),
        review_id: review_id.into(),
        candidate_tree_hash: "tree123".into(),
        diff_digest: "diff123".into(),
        review_decision_digest: format!("digest-{review_id}"),
        approved_at: Utc::now(),
    };
    let admission = svc.validate_admission(&approved).await.unwrap();
    assert!(
        matches!(admission, CommitAdmission::Blocked { .. }),
        "Expected Blocked, got: {admission:?}"
    );
}

#[tokio::test]
async fn test_admission_valid_admitted() {
    let db = setup_db().await;
    let candidate_id = format!("cand-{}", Uuid::new_v4());
    seed_candidate(
        &db.pool,
        &candidate_id,
        "executor1",
        "base123",
        "tree123",
        "diff123",
    )
    .await;
    let review_id = "rev-valid";
    seed_review(&db.pool, review_id, &candidate_id, "reviewer1", "approved").await;

    let svc = ControlledCommitService::new(db.pool.clone());
    let approved = ApprovedCandidate {
        candidate_id: candidate_id.clone(),
        review_id: review_id.into(),
        candidate_tree_hash: "tree123".into(),
        diff_digest: "diff123".into(),
        review_decision_digest: format!("digest-{review_id}"),
        approved_at: Utc::now(),
    };
    let admission = svc.validate_admission(&approved).await.unwrap();
    assert_eq!(admission, CommitAdmission::Admitted);
}

// ── Commit Creation Tests ─────────────────────────────────────────────

#[tokio::test]
async fn test_create_commit_stable_oid() {
    let db = setup_db().await;
    let (repo_dir, base_oid) = setup_git_repo();
    let tree_oid = get_tree_oid(repo_dir.path());
    let repo_path = repo_dir.path().to_path_buf();

    let candidate_id = format!("cand-{}", Uuid::new_v4());
    seed_candidate(
        &db.pool,
        &candidate_id,
        "executor1",
        &base_oid,
        &tree_oid,
        "diff456",
    )
    .await;
    let review_id = "rev-stable";
    seed_review(&db.pool, review_id, &candidate_id, "reviewer1", "approved").await;

    let svc = ControlledCommitService::new(db.pool.clone());
    let approved = ApprovedCandidate {
        candidate_id: candidate_id.clone(),
        review_id: review_id.into(),
        candidate_tree_hash: tree_oid.clone(),
        diff_digest: "diff456".into(),
        review_decision_digest: format!("digest-{review_id}"),
        approved_at: Utc::now(),
    };
    let author = GitIdentity::new("Test Author", "author@test.com");
    let committer = GitIdentity::new("Test Committer", "committer@test.com");

    let outcome = svc
        .create_commit(
            &approved,
            "repo-test",
            "refs/heads/main",
            &author,
            &committer,
            "feat: test",
            &repo_path,
        )
        .await
        .unwrap();
    assert!(!outcome.recovered);
    assert_eq!(outcome.commit_candidate.tree_oid, tree_oid);
    assert_eq!(outcome.commit_candidate.parent_oid, base_oid);

    // Verify commit object exists
    let verify = Command::new("git")
        .args(["cat-file", "-e", &outcome.commit_candidate.commit_oid])
        .current_dir(&repo_path)
        .status()
        .unwrap();
    assert!(verify.success());

    // Verify trailers in commit message
    let msg = String::from_utf8_lossy(
        &Command::new("git")
            .args([
                "log",
                "-1",
                "--format=%B",
                &outcome.commit_candidate.commit_oid,
            ])
            .current_dir(&repo_path)
            .output()
            .unwrap()
            .stdout,
    )
    .to_string();
    assert!(
        msg.contains("Harness-Candidate:"),
        "missing Harness-Candidate trailer: {msg}"
    );
}

#[tokio::test]
async fn test_create_commit_idempotent_same_oid() {
    let db = setup_db().await;
    let (repo_dir, base_oid) = setup_git_repo();
    let tree_oid = get_tree_oid(repo_dir.path());
    let repo_path = repo_dir.path().to_path_buf();

    let candidate_id = format!("cand-{}", Uuid::new_v4());
    seed_candidate(
        &db.pool,
        &candidate_id,
        "executor1",
        &base_oid,
        &tree_oid,
        "diff789",
    )
    .await;
    let review_id = "rev-idem";
    seed_review(&db.pool, review_id, &candidate_id, "reviewer1", "approved").await;

    let svc = ControlledCommitService::new(db.pool.clone());
    let approved = ApprovedCandidate {
        candidate_id: candidate_id.clone(),
        review_id: review_id.into(),
        candidate_tree_hash: tree_oid.clone(),
        diff_digest: "diff789".into(),
        review_decision_digest: format!("digest-{review_id}"),
        approved_at: Utc::now(),
    };
    let author = GitIdentity::new("A", "a@test.com");
    let committer = GitIdentity::new("C", "c@test.com");

    let o1 = svc
        .create_commit(
            &approved,
            "repo-test",
            "refs/heads/main",
            &author,
            &committer,
            "msg",
            &repo_path,
        )
        .await
        .unwrap();
    let o2 = svc
        .create_commit(
            &approved,
            "repo-test",
            "refs/heads/main",
            &author,
            &committer,
            "msg",
            &repo_path,
        )
        .await
        .unwrap();
    assert_eq!(
        o1.commit_candidate.commit_oid,
        o2.commit_candidate.commit_oid
    );
    assert!(o2.recovered);
}

#[tokio::test]
async fn test_create_commit_does_not_modify_user_index() {
    let db = setup_db().await;
    let (repo_dir, base_oid) = setup_git_repo();
    let tree_oid = get_tree_oid(repo_dir.path());
    let repo_path = repo_dir.path().to_path_buf();

    let candidate_id = format!("cand-{}", Uuid::new_v4());
    seed_candidate(
        &db.pool,
        &candidate_id,
        "executor1",
        &base_oid,
        &tree_oid,
        "diff_idx",
    )
    .await;
    let review_id = "rev-idx";
    seed_review(&db.pool, review_id, &candidate_id, "reviewer1", "approved").await;

    let idx_before = String::from_utf8_lossy(
        &Command::new("git")
            .args(["ls-files", "--stage"])
            .current_dir(&repo_path)
            .output()
            .unwrap()
            .stdout,
    )
    .to_string();

    let svc = ControlledCommitService::new(db.pool.clone());
    let approved = ApprovedCandidate {
        candidate_id,
        review_id: review_id.into(),
        candidate_tree_hash: tree_oid,
        diff_digest: "diff_idx".into(),
        review_decision_digest: format!("digest-{review_id}"),
        approved_at: Utc::now(),
    };
    let author = GitIdentity::new("A", "a@t.com");
    svc.create_commit(
        &approved,
        "repo-test",
        "refs/heads/main",
        &author,
        &author,
        "msg",
        &repo_path,
    )
    .await
    .unwrap();

    let idx_after = String::from_utf8_lossy(
        &Command::new("git")
            .args(["ls-files", "--stage"])
            .current_dir(&repo_path)
            .output()
            .unwrap()
            .stdout,
    )
    .to_string();
    assert_eq!(idx_before, idx_after, "user index was modified");
}

#[tokio::test]
async fn test_create_commit_does_not_modify_worktree() {
    let db = setup_db().await;
    let (repo_dir, base_oid) = setup_git_repo();
    let tree_oid = get_tree_oid(repo_dir.path());
    let repo_path = repo_dir.path().to_path_buf();

    let candidate_id = format!("cand-{}", Uuid::new_v4());
    seed_candidate(
        &db.pool,
        &candidate_id,
        "executor1",
        &base_oid,
        &tree_oid,
        "diff_wt",
    )
    .await;
    let review_id = "rev-wt";
    seed_review(&db.pool, review_id, &candidate_id, "reviewer1", "approved").await;

    let head_before = String::from_utf8_lossy(
        &Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&repo_path)
            .output()
            .unwrap()
            .stdout,
    )
    .trim()
    .to_string();

    let svc = ControlledCommitService::new(db.pool.clone());
    let approved = ApprovedCandidate {
        candidate_id,
        review_id: review_id.into(),
        candidate_tree_hash: tree_oid,
        diff_digest: "diff_wt".into(),
        review_decision_digest: format!("digest-{review_id}"),
        approved_at: Utc::now(),
    };
    let author = GitIdentity::new("A", "a@t.com");
    svc.create_commit(
        &approved,
        "repo-test",
        "refs/heads/main",
        &author,
        &author,
        "msg",
        &repo_path,
    )
    .await
    .unwrap();

    let head_after = String::from_utf8_lossy(
        &Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&repo_path)
            .output()
            .unwrap()
            .stdout,
    )
    .trim()
    .to_string();
    assert_eq!(head_before, head_after, "HEAD was modified");

    let status = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(&repo_path)
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&status.stdout).trim().is_empty(),
        "worktree dirty after commit"
    );
}

#[tokio::test]
async fn test_recovery_git_object_before_db_write() {
    let db = setup_db().await;
    let (repo_dir, base_oid) = setup_git_repo();
    let tree_oid = get_tree_oid(repo_dir.path());
    let repo_path = repo_dir.path().to_path_buf();

    let candidate_id = format!("cand-{}", Uuid::new_v4());
    seed_candidate(
        &db.pool,
        &candidate_id,
        "executor1",
        &base_oid,
        &tree_oid,
        "diff_rec",
    )
    .await;
    let review_id = "rev-rec";
    seed_review(&db.pool, review_id, &candidate_id, "reviewer1", "approved").await;

    let svc = ControlledCommitService::new(db.pool.clone());
    let approved = ApprovedCandidate {
        candidate_id: candidate_id.clone(),
        review_id: review_id.into(),
        candidate_tree_hash: tree_oid.clone(),
        diff_digest: "diff_rec".into(),
        review_decision_digest: format!("digest-{review_id}"),
        approved_at: Utc::now(),
    };
    let author = GitIdentity::new("A", "a@t.com");

    let o1 = svc
        .create_commit(
            &approved,
            "repo-test",
            "refs/heads/main",
            &author,
            &author,
            "msg",
            &repo_path,
        )
        .await
        .unwrap();
    assert!(!o1.recovered);

    // Simulate crash: delete DB record but keep Git object
    sqlx::query("DELETE FROM commit_candidates WHERE commit_request_id = ?")
        .bind(&o1.commit_candidate.commit_request_id)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE commit_requests SET state = 'materializing' WHERE commit_request_id = ?")
        .bind(&o1.commit_candidate.commit_request_id)
        .execute(&db.pool)
        .await
        .unwrap();

    let o2 = svc
        .create_commit(
            &approved,
            "repo-test",
            "refs/heads/main",
            &author,
            &author,
            "msg",
            &repo_path,
        )
        .await
        .unwrap();
    assert!(o2.recovered, "should have recovered");
    assert_eq!(
        o1.commit_candidate.commit_oid,
        o2.commit_candidate.commit_oid
    );
}

#[tokio::test]
async fn test_create_commit_unique_candidate_per_scope() {
    let db = setup_db().await;
    let (repo_dir, base_oid) = setup_git_repo();
    let tree_oid = get_tree_oid(repo_dir.path());
    let repo_path = repo_dir.path().to_path_buf();

    let candidate_id = format!("cand-{}", Uuid::new_v4());
    seed_candidate(
        &db.pool,
        &candidate_id,
        "executor1",
        &base_oid,
        &tree_oid,
        "diff_uniq",
    )
    .await;
    let review_id = "rev-uniq";
    seed_review(&db.pool, review_id, &candidate_id, "reviewer1", "approved").await;

    let svc = ControlledCommitService::new(db.pool.clone());
    let approved = ApprovedCandidate {
        candidate_id,
        review_id: review_id.into(),
        candidate_tree_hash: tree_oid,
        diff_digest: "diff_uniq".into(),
        review_decision_digest: format!("digest-{review_id}"),
        approved_at: Utc::now(),
    };
    let author = GitIdentity::new("A", "a@t.com");

    let o1 = svc
        .create_commit(
            &approved,
            "repo-test",
            "refs/heads/main",
            &author,
            &author,
            "msg A",
            &repo_path,
        )
        .await
        .unwrap();
    let o2 = svc
        .create_commit(
            &approved,
            "repo-test",
            "refs/heads/main",
            &author,
            &author,
            "msg A",
            &repo_path,
        )
        .await
        .unwrap();
    // Same scope → same CommitCandidate
    assert_eq!(
        o1.commit_candidate.commit_oid,
        o2.commit_candidate.commit_oid
    );
    assert_eq!(
        o1.commit_candidate.commit_request_id,
        o2.commit_candidate.commit_request_id
    );
}
