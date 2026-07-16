//! I2B-3 Batch C end-to-end tests: GitDiffScopeValidator + PolicyReconciler
//! + approval persistence, against a real temporary git repository.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use harness_core::contracts::task_envelope::FileScope;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use harness_runtime::db::Database;
use harness_runtime::policy::command::CommandPolicyEngine;
use harness_runtime::policy::diff::{ChangeKind, DiffIncludes, GitDiffScopeValidator};
use harness_runtime::policy::evidence::{PolicyEvaluationRecord, PolicyEvidenceStore};
use harness_runtime::policy::file_scope::{FileScopeValidator, ScopeDecision, ScopeViolation};
use harness_runtime::policy::reconciler::{PolicyReconciler, ReconcileReason};
use harness_runtime::policy::service::{
    LeaseFencingValidator, WorkspaceAccessGuard, WorkspacePolicyService,
};
use harness_runtime::worktree::GitRunner;

fn iso_env(tmp: &Path) -> HashMap<String, String> {
    let empty_cfg = tmp.join("empty-gitconfig");
    if !empty_cfg.exists() {
        std::fs::write(&empty_cfg, "").unwrap();
    }
    HashMap::from([
        ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
        (
            "GIT_CONFIG_GLOBAL".into(),
            empty_cfg.to_string_lossy().into_owned(),
        ),
        (
            "GIT_CEILING_DIRECTORIES".into(),
            tmp.to_string_lossy().into_owned(),
        ),
        ("GIT_AUTHOR_NAME".into(), "harness-test".into()),
        ("GIT_AUTHOR_EMAIL".into(), "test@harness.local".into()),
        ("GIT_COMMITTER_NAME".into(), "harness-test".into()),
        ("GIT_COMMITTER_EMAIL".into(), "test@harness.local".into()),
    ])
}

struct Repo {
    _tmp: tempfile::TempDir,
    repo: std::path::PathBuf,
    git: GitRunner,
}

async fn repo() -> Repo {
    let tmp = tempfile::tempdir().unwrap();
    let env = iso_env(tmp.path());
    let git = GitRunner::new(tmp.path().join("git-scratch"))
        .unwrap()
        .with_env(env);
    let repo = tmp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git.run_ok(&repo, &["init"]).await.unwrap();
    std::fs::write(repo.join("README.md"), "hello\n").unwrap();
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(repo.join("src").join("a.txt"), "a\n").unwrap();
    git.run_ok(&repo, &["add", "."]).await.unwrap();
    git.run_ok(&repo, &["commit", "-m", "base"]).await.unwrap();
    Repo {
        _tmp: tmp,
        repo,
        git,
    }
}

fn scope(allowed: &[&str]) -> FileScope {
    FileScope {
        allowed_paths: allowed.iter().map(|s| s.to_string()).collect(),
        forbidden_paths: vec![],
        readable_paths: vec![],
        scope_expansion_allowed: false,
    }
}

fn validator(repo: &Repo, allowed: &[&str]) -> FileScopeValidator {
    FileScopeValidator::new(&repo.repo, scope(allowed)).unwrap()
}

fn diff_validator(repo: &Repo) -> GitDiffScopeValidator {
    GitDiffScopeValidator::new(
        GitRunner::new(repo._tmp.path().join("diff-scratch"))
            .unwrap()
            .with_env(iso_env(repo._tmp.path())),
    )
}

fn guard(fencing: i64) -> WorkspaceAccessGuard {
    WorkspaceAccessGuard {
        lease_id: "lease-test".into(),
        lease_token: "tok-test".into(),
        fencing_token: fencing,
        worktree_id: "wt-1".into(),
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        evaluator_identity: "harness-test".into(),
    }
}

struct RejectFencing;
#[async_trait::async_trait]
impl LeaseFencingValidator for RejectFencing {
    async fn validate_active_fencing(&self, _: &str, _: &str, _: i64) -> Result<(), CoreError> {
        Err(CoreError::new(
            ErrorCode::WorkspaceError,
            "mock: stale".to_string(),
            ErrorSource::System,
        ))
    }
}

#[tokio::test]
async fn diff_detects_staged_unstaged_untracked() {
    let r = repo().await;
    // staged add
    std::fs::write(r.repo.join("src").join("new.rs"), "fn main() {}\n").unwrap();
    r.git.run_ok(&r.repo, &["add", "src/new.rs"]).await.unwrap();
    // unstaged modify
    std::fs::write(r.repo.join("src").join("a.txt"), "changed\n").unwrap();
    // untracked
    std::fs::write(r.repo.join("src").join("untracked.rs"), "x\n").unwrap();

    let v = validator(&r, &["**"]);
    let dv = diff_validator(&r);
    let report = dv
        .validate(&r.repo, &v, DiffIncludes::default())
        .await
        .unwrap();
    let paths: Vec<&str> = report
        .changed_paths
        .iter()
        .map(|c| c.path.as_str())
        .collect();
    assert!(paths.contains(&"src/new.rs"));
    assert!(paths.contains(&"src/a.txt"));
    assert!(paths.contains(&"src/untracked.rs"));
    assert!(report.clean, "all under src/** → no violations: {report:?}");
}

#[tokio::test]
async fn diff_flags_out_of_scope() {
    let r = repo().await;
    std::fs::create_dir_all(r.repo.join("secret")).unwrap();
    std::fs::write(r.repo.join("secret").join("leak.txt"), "k\n").unwrap();
    r.git
        .run_ok(&r.repo, &["add", "secret/leak.txt"])
        .await
        .unwrap();

    let v = validator(&r, &["src/**"]);
    let dv = diff_validator(&r);
    let report = dv
        .validate(&r.repo, &v, DiffIncludes::default())
        .await
        .unwrap();
    assert!(!report.clean);
    assert!(report
        .violations
        .iter()
        .any(|(p, _)| p == "secret/leak.txt"));
}

#[tokio::test]
async fn diff_rename_validates_both_sides() {
    let r = repo().await;
    // rename src/a.txt -> evil.txt (out of src/** scope)
    r.git
        .run_ok(&r.repo, &["mv", "src/a.txt", "evil.txt"])
        .await
        .unwrap();

    let v = validator(&r, &["src/**"]);
    let dv = diff_validator(&r);
    let report = dv
        .validate(
            &r.repo,
            &v,
            DiffIncludes {
                staged: true,
                unstaged: false,
                untracked: false,
            },
        )
        .await
        .unwrap();
    assert!(!report.clean, "rename into out-of-scope dest must violate");
    assert!(report
        .rename_evidence
        .iter()
        .any(|re| re.to == "evil.txt" && matches!(re.to_scope, ScopeDecision::Denied(_))));
    assert!(report
        .violations
        .iter()
        .any(|(p, v)| { p == "evil.txt" && matches!(v, ScopeViolation::OutsideWriteScope) }));
    // The change kind for the destination is Renamed.
    assert!(report
        .changed_paths
        .iter()
        .any(|c| matches!(c.kind, ChangeKind::Renamed { .. })));
}

#[tokio::test]
async fn diff_binary_detected() {
    let r = repo().await;
    std::fs::write(r.repo.join("src").join("blob.bin"), vec![0u8; 128]).unwrap();
    r.git
        .run_ok(&r.repo, &["add", "src/blob.bin"])
        .await
        .unwrap();

    let v = validator(&r, &["**"]);
    let dv = diff_validator(&r);
    let report = dv
        .validate(
            &r.repo,
            &v,
            DiffIncludes {
                staged: true,
                unstaged: false,
                untracked: false,
            },
        )
        .await
        .unwrap();
    assert!(report.binary_files.iter().any(|p| p == "src/blob.bin"));
    assert!(report
        .changed_paths
        .iter()
        .any(|c| matches!(c.kind, ChangeKind::Binary)));
}

#[tokio::test]
async fn validate_workspace_diff_persists_evidence_and_fencing() {
    let r = repo().await;
    std::fs::write(r.repo.join("src").join("new.rs"), "fn main() {}\n").unwrap();
    r.git.run_ok(&r.repo, &["add", "src/new.rs"]).await.unwrap();

    let db = Database::open_in_memory().await.unwrap();
    let store = PolicyEvidenceStore::new(db.pool.clone());
    let svc = WorkspacePolicyService::new_unverified_for_tests(store);
    let v = validator(&r, &["**"]);
    let dv = diff_validator(&r);
    let g = guard(1);
    let (report, evidence) = svc
        .validate_workspace_diff(&g, &dv, &r.repo, &v, DiffIncludes::default(), None)
        .await
        .unwrap();
    assert!(report.clean);
    assert_eq!(evidence.evaluation.decision, "allowed");
    assert_eq!(evidence.evaluation.evaluation_type, "diff");

    // Stale fencing must block evidence creation entirely.
    let store2 = PolicyEvidenceStore::new(db.pool.clone());
    let svc2 = WorkspacePolicyService::new(store2, Arc::new(RejectFencing));
    let res = svc2
        .validate_workspace_diff(&g, &dv, &r.repo, &v, DiffIncludes::default(), None)
        .await;
    assert!(res.is_err());
}

#[tokio::test]
async fn reconciler_marks_stale_fencing_and_invalidates() {
    let db = Database::open_in_memory().await.unwrap();
    let store = PolicyEvidenceStore::new(db.pool.clone());
    let rec = PolicyEvaluationRecord {
        id: "pe-old".into(),
        evaluation_type: "command".into(),
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        worktree_id: Some("wt-1".into()),
        fencing_token: Some(1), // old epoch
        policy_version: 1,
        input_fingerprint: Some("fp-old".into()),
        decision: "allowed".into(),
        reasons_json: "[]".into(),
        changed_path_count: None,
        finding_count: None,
        artifact_reference: None,
        evaluator_identity: "test".into(),
        created_at: String::new(),
    };
    store.insert_evaluation(&rec).await.unwrap();

    let recon = PolicyReconciler::new(PolicyEvidenceStore::new(db.pool.clone()));
    let report = recon.reconcile("wt-1", 2, 1, true).await.unwrap();
    assert_eq!(report.marked_invalid, 1);
    assert!(matches!(
        report.findings[0].reason,
        ReconcileReason::StaleFencing { current: 2, .. }
    ));

    // The invalidated row's decision is now the sentinel `invalid`.
    let row: (String,) =
        sqlx::query_as("SELECT decision FROM policy_evaluations WHERE id = 'pe-old'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(row.0, "invalid");
}

#[tokio::test]
async fn invalidated_evidence_not_reused_for_idempotency() {
    let db = Database::open_in_memory().await.unwrap();
    let store = PolicyEvidenceStore::new(db.pool.clone());
    // An "allowed" command evidence under fencing=1, then invalidated.
    let rec = PolicyEvaluationRecord {
        id: "pe-x".into(),
        evaluation_type: "command".into(),
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        worktree_id: Some("wt-1".into()),
        fencing_token: Some(1),
        policy_version: 1,
        input_fingerprint: Some(
            CommandPolicyEngine::new()
                .fingerprint("git", &["status".into()], std::path::Path::new("/w"), &[])
                .composite_key(),
        ),
        decision: "allowed".into(),
        reasons_json: "[]".into(),
        changed_path_count: None,
        finding_count: None,
        artifact_reference: None,
        evaluator_identity: "test".into(),
        created_at: String::new(),
    };
    store.insert_evaluation(&rec).await.unwrap();
    store.mark_invalid("pe-x", "stale").await.unwrap();

    // Under the SAME fencing token, re-evaluating must NOT reuse the invalid
    // row — it produces a fresh evaluation with a different id.
    let svc =
        WorkspacePolicyService::new_unverified_for_tests(PolicyEvidenceStore::new(db.pool.clone()));
    let g = guard(1);
    let (_d, ev) = svc
        .evaluate_command(&g, "git", &["status".into()], "/w", &[])
        .await
        .unwrap();
    assert_ne!(ev.evaluation.id, "pe-x");
}

#[tokio::test]
async fn reconciler_marks_lost_artifact() {
    let db = Database::open_in_memory().await.unwrap();
    let store = PolicyEvidenceStore::new(db.pool.clone());
    let rec = PolicyEvaluationRecord {
        id: "pe-art".into(),
        evaluation_type: "diff".into(),
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        worktree_id: Some("wt-1".into()),
        fencing_token: Some(5),
        policy_version: 1,
        input_fingerprint: None,
        decision: "allowed".into(),
        reasons_json: "[]".into(),
        changed_path_count: Some(3),
        finding_count: None,
        artifact_reference: Some("/no/such/spool/path.diff".into()),
        evaluator_identity: "test".into(),
        created_at: String::new(),
    };
    store.insert_evaluation(&rec).await.unwrap();

    let recon = PolicyReconciler::new(PolicyEvidenceStore::new(db.pool.clone()));
    let report = recon.reconcile("wt-1", 5, 1, true).await.unwrap();
    assert_eq!(report.marked_invalid, 1);
    assert!(matches!(
        report.findings[0].reason,
        ReconcileReason::ArtifactLost(_)
    ));
}

#[tokio::test]
async fn approval_persistence_roundtrip() {
    let db = Database::open_in_memory().await.unwrap();
    let store = PolicyEvidenceStore::new(db.pool.clone());
    let svc = WorkspacePolicyService::new_unverified_for_tests(store);

    let g = guard(1);
    let fp = CommandPolicyEngine::new().fingerprint(
        "git",
        &["push".into()],
        std::path::Path::new("/w"),
        &[],
    );
    svc.record_approval(&g, &fp, "approved", None)
        .await
        .unwrap();

    // Same epoch → found.
    let found = svc.find_approval(&g, &fp).await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().decision, "approved");

    // Different (stale) epoch → not found.
    let g2 = guard(2);
    let none = svc.find_approval(&g2, &fp).await.unwrap();
    assert!(none.is_none(), "stale-epoch approval must not be reusable");
}
