//! I2B-1 WorktreeManager integration tests.
//!
//! Every test uses throwaway git repositories under a tempdir and an
//! isolated git environment (`GIT_CONFIG_NOSYSTEM` + empty
//! `GIT_CONFIG_GLOBAL`) — the user's global git configuration is never read
//! for behavior-sensitive settings and never modified.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use harness_runtime::db::Database;
use harness_runtime::operation::OperationManager;
use harness_runtime::worktree::{
    lock::RepositoryLocks, metadata, naming, GitRunner, RepositoryInspector, WorktreeCreateOutcome,
    WorktreeDriftKind, WorktreeManager, WorktreeMetadata, WorktreeReconciler, WorktreeRecord,
    WorktreeRemoveOutcome, WorktreeRemovePolicy, WorktreeSpec, WorktreeStatus,
};

struct TestEnv {
    _tmp: tempfile::TempDir,
    db: Database,
    repo: PathBuf,
    worktree_root: PathBuf,
    manager: WorktreeManager,
    git: GitRunner,
    head: String,
    env: HashMap<String, String>,
}

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
        // Keep discovery inside the tempdir — the developer's HOME may itself
        // be a git repository, which must not leak into these tests.
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

async fn setup() -> TestEnv {
    let tmp = tempfile::tempdir().unwrap();
    let env = iso_env(tmp.path());

    let git = GitRunner::new(tmp.path().join("git-scratch"))
        .unwrap()
        .with_env(env.clone());

    let repo = tmp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git.run_ok(&repo, &["init"]).await.unwrap();
    git.run_ok(&repo, &["commit", "--allow-empty", "-m", "init"])
        .await
        .unwrap();
    std::fs::write(repo.join("README.md"), "hello\n").unwrap();
    git.run_ok(&repo, &["add", "."]).await.unwrap();
    git.run_ok(&repo, &["commit", "-m", "base"]).await.unwrap();
    let head = git.run_ok(&repo, &["rev-parse", "HEAD"]).await.unwrap();

    let db = Database::open_in_memory().await.unwrap();

    let inspector_git = GitRunner::new(tmp.path().join("git-scratch-mgr"))
        .unwrap()
        .with_env(env.clone());
    let worktree_root = tmp.path().join("wt-root");
    let manager = WorktreeManager::new_unleased(
        db.pool.clone(),
        RepositoryInspector::new(inspector_git),
        &worktree_root,
        "sup-test".into(),
    )
    .unwrap();

    TestEnv {
        db,
        repo,
        worktree_root: naming::canonicalize_for_git(&worktree_root).unwrap(),
        manager,
        git,
        head: head.trim().to_string(),
        env,
        _tmp: tmp,
    }
}

async fn seed_task(env: &TestEnv, project_id: &str, task_id: &str) {
    sqlx::query(
        "INSERT OR IGNORE INTO projects (id, objective, lifecycle) VALUES (?,'test','active')",
    )
    .bind(project_id)
    .execute(&env.db.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT OR IGNORE INTO tasks (id, project_id, goal, lifecycle) VALUES (?,?,'test','running')",
    )
    .bind(task_id)
    .bind(project_id)
    .execute(&env.db.pool)
    .await
    .unwrap();
}

fn spec(env: &TestEnv, task_id: &str, execution_id: &str) -> WorktreeSpec {
    WorktreeSpec {
        project_id: "p1".into(),
        task_id: task_id.into(),
        execution_id: execution_id.into(),
        repository_root: env.repo.clone(),
        base_commit: env.head.clone(),
        worktree_path: naming::default_worktree_path(
            &env.worktree_root,
            "p1",
            task_id,
            execution_id,
        )
        .unwrap(),
        branch_name: naming::branch_name(task_id, execution_id).unwrap(),
        operation_id: format!("op-create-{task_id}-{execution_id}"),
        owner_supervisor_id: "sup-test".into(),
    }
}

async fn create(env: &TestEnv, task_id: &str, execution_id: &str) -> WorktreeRecord {
    seed_task(env, "p1", task_id).await;
    let s = spec(env, task_id, execution_id);
    match env.manager.create_worktree(&s).await.unwrap() {
        WorktreeCreateOutcome::Created(r) => r,
        other => panic!("expected Created, got {other:?}"),
    }
}

fn reconciler(env: &TestEnv) -> WorktreeReconciler {
    let git = GitRunner::new(env._tmp.path().join("git-scratch-recon"))
        .unwrap()
        .with_env(env.env.clone());
    WorktreeReconciler::new(
        env.db.pool.clone(),
        RepositoryInspector::new(git),
        env.worktree_root.clone(),
        "sup-test".into(),
    )
}

// ── 1-5: RepositoryInspector ─────────────────────────────────────

#[tokio::test]
async fn non_git_path_rejected() {
    let env = setup().await;
    let plain = env._tmp.path().join("not-a-repo");
    std::fs::create_dir_all(&plain).unwrap();
    let result = env.manager.inspector().locate_repository(&plain).await;
    assert!(result.is_err(), "non-git path must be rejected");
}

#[tokio::test]
async fn repository_root_inspection() {
    let env = setup().await;
    let sub = env.repo.join("subdir");
    std::fs::create_dir_all(&sub).unwrap();
    let facts = env
        .manager
        .inspector()
        .locate_repository(&sub)
        .await
        .unwrap();
    assert_eq!(
        facts.repository_root,
        naming::canonicalize_for_git(&env.repo).unwrap()
    );
    assert!(!facts.is_bare);
    assert!(facts.supports_worktrees, "modern git supports worktrees");
    assert!(facts.git_version.starts_with("git version"));
    assert!(facts.common_git_dir.ends_with(".git"));
}

#[tokio::test]
async fn head_and_base_commit_resolution() {
    let env = setup().await;
    let facts = env
        .manager
        .inspector()
        .locate_repository(&env.repo)
        .await
        .unwrap();
    assert_eq!(facts.head_commit.as_deref(), Some(env.head.as_str()));
    let resolved = env
        .manager
        .inspector()
        .resolve_commit(&env.repo, "HEAD")
        .await
        .unwrap();
    assert_eq!(resolved, env.head);
    assert_eq!(resolved.len(), 40, "full OID expected");
}

#[tokio::test]
async fn dirty_repository_detection() {
    let env = setup().await;
    assert!(!env.manager.inspector().is_dirty(&env.repo).await.unwrap());
    std::fs::write(env.repo.join("dirty.txt"), "x").unwrap();
    assert!(env.manager.inspector().is_dirty(&env.repo).await.unwrap());
}

#[tokio::test]
async fn worktree_porcelain_listing() {
    let env = setup().await;
    let record = create(&env, "t5", "e1").await;
    let entries = env
        .manager
        .inspector()
        .list_worktrees(&env.repo)
        .await
        .unwrap();
    assert!(entries.len() >= 2, "main + linked worktree expected");
    assert!(entries
        .iter()
        .any(|e| e.branch.as_deref() == Some(record.branch_name.as_str())));
}

// ── 6-7: naming policy (integration-level) ───────────────────────

#[tokio::test]
async fn branch_name_sanitization_via_git() {
    let env = setup().await;
    assert!(env
        .manager
        .inspector()
        .check_branch_name(&env.repo, "harness/t1/e1")
        .await
        .unwrap());
    for bad in ["has space", "a..b", "end.lock", "-dash"] {
        assert!(
            !env.manager
                .inspector()
                .check_branch_name(&env.repo, bad)
                .await
                .unwrap(),
            "{bad:?} must be rejected by git"
        );
    }
}

#[tokio::test]
async fn path_escape_rejected_on_create() {
    let env = setup().await;
    seed_task(&env, "p1", "t7").await;
    let mut s = spec(&env, "t7", "e1");
    s.worktree_path = env._tmp.path().join("outside-root");
    assert!(env.manager.create_worktree(&s).await.is_err());
    let mut s2 = spec(&env, "t7", "e2");
    s2.worktree_path = env.worktree_root.join("..").join("escape");
    assert!(env.manager.create_worktree(&s2).await.is_err());
}

// ── 8-11: create ─────────────────────────────────────────────────

#[tokio::test]
async fn create_worktree_succeeds() {
    let env = setup().await;
    let record = create(&env, "t8", "e1").await;
    assert!(PathBuf::from(&record.worktree_path).exists());
    assert_eq!(record.status, WorktreeStatus::Active);
    assert_eq!(record.base_commit, env.head);
}

#[tokio::test]
async fn create_head_is_base_commit() {
    let env = setup().await;
    let record = create(&env, "t9", "e1").await;
    let inspection = env.manager.inspect_worktree(&record).await.unwrap();
    assert_eq!(inspection.head_commit.as_deref(), Some(env.head.as_str()));
    assert!(inspection.head_equals_base);
    assert!(inspection.head_descends_from_base);
}

#[tokio::test]
async fn create_branch_is_correct() {
    let env = setup().await;
    let record = create(&env, "t10", "e1").await;
    let inspection = env.manager.inspect_worktree(&record).await.unwrap();
    assert_eq!(inspection.branch.as_deref(), Some("harness/t10/e1"));
    assert!(inspection.branch_matches);
}

#[tokio::test]
async fn create_ownership_metadata_correct() {
    let env = setup().await;
    let record = create(&env, "t11", "e1").await;
    let path = PathBuf::from(&record.worktree_path);
    let meta = metadata::read_sidecar(&path).unwrap().expect("sidecar");
    assert!(meta.matches_record(&record));
    assert_eq!(meta.owner_supervisor_id, "sup-test");
    assert_eq!(meta.state, "active");
    assert_eq!(meta.schema_version, 1);
    // The sidecar must NOT be inside the worktree (no diff pollution).
    assert!(!metadata::sidecar_path(&path).starts_with(&path));
    assert!(!env.manager.inspector().is_dirty(&path).await.unwrap());
}

// ── 12-16: create edge cases ─────────────────────────────────────

#[tokio::test]
async fn repeated_create_is_idempotent() {
    let env = setup().await;
    let record = create(&env, "t12", "e1").await;
    let s = spec(&env, "t12", "e1");
    match env.manager.create_worktree(&s).await.unwrap() {
        WorktreeCreateOutcome::AlreadyExists(r) => assert_eq!(r.worktree_id, record.worktree_id),
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
    // Exactly one worktree directory exists for the task.
    let parent = PathBuf::from(&record.worktree_path);
    let dirs: Vec<_> = std::fs::read_dir(parent.parent().unwrap())
        .unwrap()
        .flatten()
        .filter(|e| e.path().is_dir())
        .collect();
    assert_eq!(dirs.len(), 1);
}

#[tokio::test]
async fn concurrent_create_only_one_succeeds() {
    let env = setup().await;
    seed_task(&env, "p1", "t13").await;
    let s1 = spec(&env, "t13", "e1");
    let s2 = s1.clone();
    let (r1, r2) = tokio::join!(
        env.manager.create_worktree(&s1),
        env.manager.create_worktree(&s2),
    );
    let created = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, Ok(WorktreeCreateOutcome::Created(_))))
        .count();
    assert_eq!(
        created, 1,
        "exactly one concurrent create may perform the side effect: {r1:?} {r2:?}"
    );
    assert!(s1.worktree_path.exists());
}

#[tokio::test]
async fn nonexistent_base_commit_rejected() {
    let env = setup().await;
    seed_task(&env, "p1", "t14").await;
    let mut s = spec(&env, "t14", "e1");
    s.base_commit = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".into();
    let err = env.manager.create_worktree(&s).await;
    assert!(err.is_err());
    assert!(!s.worktree_path.exists(), "no side effect on failure");
}

#[tokio::test]
async fn existing_target_path_rejected() {
    let env = setup().await;
    seed_task(&env, "p1", "t15").await;
    let s = spec(&env, "t15", "e1");
    std::fs::create_dir_all(&s.worktree_path).unwrap();
    assert!(env.manager.create_worktree(&s).await.is_err());
}

#[tokio::test]
async fn branch_collision_rejected() {
    let env = setup().await;
    seed_task(&env, "p1", "t16").await;
    let s = spec(&env, "t16", "e1");
    env.git
        .run_ok(&env.repo, &["branch", &s.branch_name, &env.head])
        .await
        .unwrap();
    assert!(env.manager.create_worktree(&s).await.is_err());
    assert!(!s.worktree_path.exists());
}

// ── 17-18: inspect ───────────────────────────────────────────────

#[tokio::test]
async fn inspect_clean_worktree() {
    let env = setup().await;
    let record = create(&env, "t17", "e1").await;
    let i = env.manager.inspect_worktree(&record).await.unwrap();
    assert!(i.path_exists);
    assert!(i.belongs_to_repository);
    assert!(i.metadata_present && i.metadata_matches);
    assert_eq!(i.dirty, Some(false));
    assert!(!i.locked && !i.prunable && !i.git_admin_missing && !i.moved_or_deleted);
}

#[tokio::test]
async fn inspect_dirty_worktree() {
    let env = setup().await;
    let record = create(&env, "t18", "e1").await;
    std::fs::write(PathBuf::from(&record.worktree_path).join("change.txt"), "x").unwrap();
    let i = env.manager.inspect_worktree(&record).await.unwrap();
    assert_eq!(i.dirty, Some(true));
}

// ── 19-23: remove ────────────────────────────────────────────────

#[tokio::test]
async fn dirty_worktree_removal_refused_by_default() {
    let env = setup().await;
    let record = create(&env, "t19", "e1").await;
    let path = PathBuf::from(&record.worktree_path);
    std::fs::write(path.join("dirty.txt"), "x").unwrap();
    let outcome = env
        .manager
        .remove_worktree(&record.worktree_id, WorktreeRemovePolicy::default())
        .await
        .unwrap();
    assert!(
        matches!(outcome, WorktreeRemoveOutcome::RefusedDirty { changed_entries } if changed_entries >= 1)
    );
    assert!(path.exists(), "dirty worktree must not be deleted");
}

#[tokio::test]
async fn clean_worktree_removed() {
    let env = setup().await;
    let record = create(&env, "t20", "e1").await;
    let path = PathBuf::from(&record.worktree_path);
    let outcome = env
        .manager
        .remove_worktree(&record.worktree_id, WorktreeRemovePolicy::default())
        .await
        .unwrap();
    assert_eq!(outcome, WorktreeRemoveOutcome::Removed);
    assert!(!path.exists());
    // Git no longer registers it.
    let entries = env
        .manager
        .inspector()
        .list_worktrees(&env.repo)
        .await
        .unwrap();
    assert!(!entries.iter().any(|e| e.path == path));
    // Record final, diagnostics tombstone kept.
    let record = env
        .manager
        .get_record(&record.worktree_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, WorktreeStatus::Removed);
    assert!(metadata::diagnostics_path(&path).exists());
}

#[tokio::test]
async fn repeated_remove_is_idempotent() {
    let env = setup().await;
    let record = create(&env, "t21", "e1").await;
    let first = env
        .manager
        .remove_worktree(&record.worktree_id, WorktreeRemovePolicy::default())
        .await
        .unwrap();
    assert_eq!(first, WorktreeRemoveOutcome::Removed);
    let second = env
        .manager
        .remove_worktree(&record.worktree_id, WorktreeRemovePolicy::default())
        .await
        .unwrap();
    assert_eq!(second, WorktreeRemoveOutcome::AlreadyRemoved);
}

#[tokio::test]
async fn unverifiable_ownership_removal_refused() {
    let env = setup().await;
    let record = create(&env, "t22", "e1").await;
    let path = PathBuf::from(&record.worktree_path);
    // Simulate a worktree we cannot prove ownership of: sidecar gone.
    std::fs::remove_file(metadata::sidecar_path(&path)).unwrap();
    let outcome = env
        .manager
        .remove_worktree(&record.worktree_id, WorktreeRemovePolicy::default())
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        WorktreeRemoveOutcome::RefusedOwnershipUnverified { .. }
    ));
    assert!(path.exists(), "unverified worktree must never be deleted");
    let record = env
        .manager
        .get_record(&record.worktree_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, WorktreeStatus::ReconciliationRequired);
}

#[tokio::test]
async fn repository_root_removal_refused() {
    let env = setup().await;
    seed_task(&env, "p1", "t23").await;
    let repo_canonical = naming::canonicalize_for_git(&env.repo).unwrap();
    let facts = env
        .manager
        .inspector()
        .locate_repository(&env.repo)
        .await
        .unwrap();
    // Forge a record + matching sidecar pointing at the repository root.
    let record = WorktreeRecord {
        worktree_id: "wt-t23-forged".into(),
        project_id: "p1".into(),
        task_id: "t23".into(),
        execution_id: "e1".into(),
        repository_root: repo_canonical.to_string_lossy().into_owned(),
        repository_identity: facts.common_git_dir.to_string_lossy().into_owned(),
        worktree_path: repo_canonical.to_string_lossy().into_owned(),
        branch_name: "main-or-master".into(),
        base_commit: env.head.clone(),
        owner_supervisor_id: "sup-test".into(),
        operation_id: "op-forged".into(),
        status: WorktreeStatus::Active,
        created_at: "2026-07-15 00:00:00".into(),
    };
    sqlx::query("INSERT INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status) VALUES (?,?,?,?,?,?,?,?,?,?,?,'active')")
        .bind(&record.worktree_id).bind(&record.project_id).bind(&record.task_id).bind(&record.execution_id)
        .bind(&record.repository_root).bind(&record.repository_identity).bind(&record.worktree_path)
        .bind(&record.branch_name).bind(&record.base_commit).bind(&record.owner_supervisor_id).bind(&record.operation_id)
        .execute(&env.db.pool).await.unwrap();
    metadata::write_sidecar(&repo_canonical, &WorktreeMetadata::from_record(&record)).unwrap();

    let result = env
        .manager
        .remove_worktree("wt-t23-forged", WorktreeRemovePolicy { force_dirty: true })
        .await;
    assert!(result.is_err(), "repository root must never be removed");
    assert!(env.repo.join("README.md").exists(), "repository intact");
}

// ── 24-29: reconciliation ────────────────────────────────────────

#[tokio::test]
async fn reconcile_create_crash_before_db_write() {
    let env = setup().await;
    seed_task(&env, "p1", "t24").await;
    let s = spec(&env, "t24", "e1");
    let worktree_id = naming::worktree_id("t24", "e1").unwrap();

    // Simulate the crash window: intent + git side effect + sidecar written,
    // but no DB record and the operation never completed.
    let ops = OperationManager::new(env.db.pool.clone());
    let op_id = ops
        .begin(
            "t24",
            "worktree_create",
            &serde_json::json!({ "worktree_id": worktree_id, "spec": s }),
            &s.operation_id,
        )
        .await
        .unwrap();
    let path_str = s.worktree_path.to_string_lossy().into_owned();
    env.git
        .run_ok(
            &env.repo,
            &[
                "worktree",
                "add",
                "-b",
                &s.branch_name,
                &path_str,
                &env.head,
            ],
        )
        .await
        .unwrap();
    let canonical = naming::canonicalize_for_git(&s.worktree_path).unwrap();
    let facts = env
        .manager
        .inspector()
        .locate_repository(&env.repo)
        .await
        .unwrap();
    let record = WorktreeRecord {
        worktree_id: worktree_id.clone(),
        project_id: "p1".into(),
        task_id: "t24".into(),
        execution_id: "e1".into(),
        repository_root: facts.repository_root.to_string_lossy().into_owned(),
        repository_identity: facts.common_git_dir.to_string_lossy().into_owned(),
        worktree_path: canonical.to_string_lossy().into_owned(),
        branch_name: s.branch_name.clone(),
        base_commit: env.head.clone(),
        owner_supervisor_id: "sup-test".into(),
        operation_id: op_id.clone(),
        status: WorktreeStatus::Active,
        created_at: "2026-07-15 00:00:00".into(),
    };
    metadata::write_sidecar(&canonical, &WorktreeMetadata::from_record(&record)).unwrap();

    let drifts = reconciler(&env).reconcile().await.unwrap();
    let repaired = drifts
        .iter()
        .any(|d| d.kind == WorktreeDriftKind::IncompleteCreateOperation && d.repaired);
    assert!(
        repaired,
        "create must be completed by reconciliation: {drifts:?}"
    );
    let record = env
        .manager
        .get_record(&worktree_id)
        .await
        .unwrap()
        .expect("record persisted");
    assert_eq!(record.status, WorktreeStatus::Active);
    let (status,): (String,) =
        sqlx::query_as("SELECT status FROM operations WHERE operation_id = ?")
            .bind(&op_id)
            .fetch_one(&env.db.pool)
            .await
            .unwrap();
    assert_eq!(status, "completed");
}

#[tokio::test]
async fn reconcile_remove_crash_before_db_write() {
    let env = setup().await;
    let record = create(&env, "t25", "e1").await;
    let path = PathBuf::from(&record.worktree_path);

    // Simulate: remove intent recorded, git side effect done, then crash.
    let ops = OperationManager::new(env.db.pool.clone());
    let op_id = ops
        .begin(
            "t25",
            "worktree_remove",
            &serde_json::json!({ "worktree_id": record.worktree_id }),
            &format!("wt-remove-{}", record.worktree_id),
        )
        .await
        .unwrap();
    let path_str = path.to_string_lossy().into_owned();
    env.git
        .run_ok(&env.repo, &["worktree", "remove", &path_str])
        .await
        .unwrap();
    assert!(!path.exists());

    let drifts = reconciler(&env).reconcile().await.unwrap();
    let repaired = drifts
        .iter()
        .any(|d| d.kind == WorktreeDriftKind::IncompleteRemoveOperation && d.repaired);
    assert!(
        repaired,
        "remove must be completed by reconciliation: {drifts:?}"
    );
    let record = env
        .manager
        .get_record(&record.worktree_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, WorktreeStatus::Removed);
    let (status,): (String,) =
        sqlx::query_as("SELECT status FROM operations WHERE operation_id = ?")
            .bind(&op_id)
            .fetch_one(&env.db.pool)
            .await
            .unwrap();
    assert_eq!(status, "completed");
}

#[tokio::test]
async fn reconcile_db_record_with_missing_fs() {
    let env = setup().await;
    let record = create(&env, "t26", "e1").await;
    let path = PathBuf::from(&record.worktree_path);
    std::fs::remove_dir_all(&path).unwrap();

    let drifts = reconciler(&env).reconcile().await.unwrap();
    assert!(drifts
        .iter()
        .any(|d| d.kind == WorktreeDriftKind::DbPresentFsMissing
            && d.worktree_id.as_deref() == Some(record.worktree_id.as_str())));
    let record = env
        .manager
        .get_record(&record.worktree_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, WorktreeStatus::ReconciliationRequired);
}

#[tokio::test]
async fn reconcile_git_worktree_with_missing_metadata() {
    let env = setup().await;
    let record = create(&env, "t27", "e1").await;
    let path = PathBuf::from(&record.worktree_path);
    std::fs::remove_file(metadata::sidecar_path(&path)).unwrap();

    let drifts = reconciler(&env).reconcile().await.unwrap();
    assert!(drifts
        .iter()
        .any(|d| d.kind == WorktreeDriftKind::GitPresentMetadataMissing));
    assert!(path.exists(), "diagnosis must not delete anything");
    let record = env
        .manager
        .get_record(&record.worktree_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, WorktreeStatus::ReconciliationRequired);
}

#[tokio::test]
async fn reconcile_owner_mismatch_never_deletes() {
    let env = setup().await;
    let record = create(&env, "t28", "e1").await;
    let path = PathBuf::from(&record.worktree_path);
    let mut meta = metadata::read_sidecar(&path).unwrap().unwrap();
    meta.owner_supervisor_id = "sup-someone-else".into();
    metadata::write_sidecar(&path, &meta).unwrap();

    let drifts = reconciler(&env).reconcile().await.unwrap();
    let mismatch = drifts
        .iter()
        .find(|d| d.kind == WorktreeDriftKind::OwnerMismatch)
        .expect("owner mismatch drift");
    assert!(!mismatch.repaired, "owner mismatch must be diagnostic only");
    assert!(
        path.exists(),
        "worktree must never be auto-deleted on owner mismatch"
    );
}

#[tokio::test]
async fn reconcile_stale_temp_entries_cleaned() {
    let env = setup().await;
    let project_dir = env.worktree_root.join("p1");
    std::fs::create_dir_all(&project_dir).unwrap();
    let stale_file = project_dir.join("t99-e1.harness.json.tmp");
    std::fs::write(&stale_file, "{}").unwrap();
    let stale_dir = project_dir.join(".wt-tmp-abc123");
    std::fs::create_dir_all(&stale_dir).unwrap();

    let drifts = reconciler(&env).reconcile().await.unwrap();
    let cleaned: Vec<_> = drifts
        .iter()
        .filter(|d| d.kind == WorktreeDriftKind::StaleTempDirectory && d.repaired)
        .collect();
    assert_eq!(cleaned.len(), 2, "{drifts:?}");
    assert!(!stale_file.exists());
    assert!(!stale_dir.exists());
}

// ── 30: repository-scoped lock ───────────────────────────────────

#[tokio::test]
async fn repo_lock_serializes_same_repo_and_parallelizes_different() {
    let locks = Arc::new(RepositoryLocks::new());

    let guard_a = locks.acquire("repo-A").await;

    // Same repo: a second acquire must block while the guard is held.
    let locks2 = locks.clone();
    let contender = tokio::spawn(async move {
        let _g = locks2.acquire("repo-A").await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!contender.is_finished(), "same-repo acquire must wait");

    // Different repo: proceeds immediately even while repo-A is held.
    let got_b = tokio::time::timeout(Duration::from_millis(200), locks.acquire("repo-B")).await;
    assert!(got_b.is_ok(), "different repositories must run in parallel");

    drop(guard_a);
    tokio::time::timeout(Duration::from_secs(2), contender)
        .await
        .expect("same-repo contender must proceed after release")
        .unwrap();
}
