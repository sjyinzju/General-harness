//! WorktreeManager — create / inspect / remove task worktrees.
//!
//! Every side effect runs inside an Operation/Saga (persisted intent → claim
//! → git side effect → verify → complete) and under the repository-scoped
//! administrative lock. Identical requests (same `operation_id`) are
//! idempotent: they can never create a second worktree or double-remove.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

use super::inspector::RepositoryInspector;
use super::lock::RepositoryLocks;
use super::metadata::{self, WorktreeMetadata};
use super::naming;
use super::types::*;
use crate::operation::OperationManager;

pub struct WorktreeManager {
    pool: SqlitePool,
    inspector: RepositoryInspector,
    locks: Arc<RepositoryLocks>,
    ops: OperationManager,
    worktree_root: PathBuf,
    supervisor_id: String,
}

impl WorktreeManager {
    /// `worktree_root` is the harness-owned data directory for worktrees.
    /// It must NOT live inside a user git worktree (tracked directory).
    pub fn new(
        pool: SqlitePool,
        inspector: RepositoryInspector,
        worktree_root: &Path,
        supervisor_id: String,
    ) -> Result<Self, CoreError> {
        if let Some(ancestor) = crate::artifact::find_git_ancestor(worktree_root) {
            return Err(ws_err(format!(
                "worktree root {} is inside a git worktree ({}); configure a harness data directory instead",
                worktree_root.display(),
                ancestor.display()
            )));
        }
        std::fs::create_dir_all(worktree_root)
            .map_err(|e| ws_err(format!("create worktree root: {e}")))?;
        let root = naming::canonicalize_for_git(worktree_root)
            .map_err(|e| ws_err(format!("canonicalize worktree root: {e}")))?;
        Ok(Self {
            ops: OperationManager::new(pool.clone()),
            pool,
            inspector,
            locks: Arc::new(RepositoryLocks::new()),
            worktree_root: root,
            supervisor_id,
        })
    }

    pub fn worktree_root(&self) -> &Path {
        &self.worktree_root
    }

    pub fn inspector(&self) -> &RepositoryInspector {
        &self.inspector
    }

    // ── Create ───────────────────────────────────────────────────

    pub async fn create_worktree(
        &self,
        spec: &WorktreeSpec,
    ) -> Result<WorktreeCreateOutcome, CoreError> {
        // 1. Static validation before any side effect.
        naming::validate_identifier(&spec.project_id)?;
        naming::validate_identifier(&spec.task_id)?;
        naming::validate_identifier(&spec.execution_id)?;
        naming::validate_branch_name(&spec.branch_name)?;
        naming::ensure_under_root(&self.worktree_root, &spec.worktree_path)?;
        let worktree_id = naming::worktree_id(&spec.task_id, &spec.execution_id)?;
        tracing::debug!(
            worktree_id = %worktree_id,
            supervisor = %self.supervisor_id,
            owner = %spec.owner_supervisor_id,
            "worktree_create_requested"
        );

        // 2. Record intent (Operation). Duplicate operation_id → idempotent path.
        let payload = serde_json::json!({
            "worktree_id": worktree_id,
            "spec": spec,
        });
        let op_id = match self
            .ops
            .begin(
                &spec.task_id,
                "worktree_create",
                &payload,
                &spec.operation_id,
            )
            .await
        {
            Ok(id) => id,
            Err(_) => {
                // Existing operation with the same idempotency key.
                let Some((existing_op, status)) =
                    find_op_by_ikey(&self.pool, &spec.operation_id).await?
                else {
                    return Err(ws_err(
                        "operation insert failed without existing key".into(),
                    ));
                };
                match status.as_str() {
                    "completed" => {
                        let record = self.get_record(&worktree_id).await?.ok_or_else(|| {
                            ws_err(format!(
                                "operation {existing_op} completed but record {worktree_id} missing (reconciliation required)"
                            ))
                        })?;
                        return Ok(WorktreeCreateOutcome::AlreadyExists(record));
                    }
                    "failed" => {
                        return Err(ws_err(format!(
                            "previous create operation {existing_op} failed; use a new operation_id"
                        )));
                    }
                    _ => existing_op, // pending/running/reconciliation_required → try to take over
                }
            }
        };

        // 3. Claim the operation (fencing: only one owner executes).
        let Some(token) = self.ops.try_claim_operation(&op_id, 60).await? else {
            return Ok(WorktreeCreateOutcome::InProgress);
        };

        match self
            .create_claimed(spec, &worktree_id, &op_id, &token)
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(e) => {
                let _ = self
                    .ops
                    .fail_claimed_operation(&op_id, &token, &e.message)
                    .await;
                Err(e)
            }
        }
    }

    async fn create_claimed(
        &self,
        spec: &WorktreeSpec,
        worktree_id: &str,
        op_id: &str,
        token: &str,
    ) -> Result<WorktreeCreateOutcome, CoreError> {
        // 4. Repository facts + preconditions.
        let facts = self
            .inspector
            .locate_repository(&spec.repository_root)
            .await?;
        if facts.is_bare {
            return Err(ws_err(
                "bare repositories are not supported for task worktrees".into(),
            ));
        }
        if !facts.supports_worktrees {
            return Err(ws_err(format!(
                "git does not support worktree porcelain: {}",
                facts.git_version
            )));
        }
        // Branch name must pass git's own validation.
        if !self
            .inspector
            .check_branch_name(&facts.repository_root, &spec.branch_name)
            .await?
        {
            return Err(ws_err(format!(
                "branch name rejected by git check-ref-format: {}",
                spec.branch_name
            )));
        }
        let base_oid = self
            .inspector
            .resolve_commit(&facts.repository_root, &spec.base_commit)
            .await?;
        let repo_identity = facts.common_git_dir.to_string_lossy().into_owned();

        // 5. Serialize administrative operations per repository.
        let _repo_guard = self.locks.acquire(&repo_identity).await;

        // 6. Pre-flight & crash-resume detection.
        let listed = self
            .inspector
            .list_worktrees(&facts.repository_root)
            .await?;
        let target_canonical = naming::canonicalize_for_git(&spec.worktree_path).ok();
        let already_listed = target_canonical.is_some()
            && listed.iter().any(|e| {
                naming::canonicalize_for_git(&e.path)
                    .ok()
                    .or_else(|| Some(e.path.clone()))
                    .as_deref()
                    == target_canonical.as_deref()
            });

        if already_listed {
            // A worktree already sits at the target path. Resume only when
            // our own metadata proves it is this very request.
            let sidecar = metadata::read_sidecar(&spec.worktree_path)?;
            match sidecar {
                Some(meta) if meta.worktree_id == worktree_id => {
                    let record = self
                        .verify_and_persist(
                            spec,
                            worktree_id,
                            op_id,
                            token,
                            &facts.repository_root,
                            &repo_identity,
                            &base_oid,
                        )
                        .await?;
                    return Ok(WorktreeCreateOutcome::Created(record));
                }
                _ => {
                    return Err(ws_err(format!(
                        "target path is occupied by a worktree not owned by this request: {}",
                        spec.worktree_path.display()
                    )));
                }
            }
        }
        if spec.worktree_path.exists() {
            return Err(ws_err(format!(
                "target path already exists: {}",
                spec.worktree_path.display()
            )));
        }
        if self
            .inspector
            .branch_exists(&facts.repository_root, &spec.branch_name)
            .await?
        {
            return Err(ws_err(format!(
                "branch already exists: {}",
                spec.branch_name
            )));
        }

        // 7. Side effect: git worktree add -b <branch> <path> <base_oid>.
        if let Some(parent) = spec.worktree_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| ws_err(format!("create worktree parent: {e}")))?;
        }
        let path_str = spec.worktree_path.to_string_lossy().into_owned();
        let add = self
            .inspector
            .git()
            .run(
                &facts.repository_root,
                &[
                    "worktree",
                    "add",
                    "-b",
                    &spec.branch_name,
                    &path_str,
                    &base_oid,
                ],
            )
            .await?;
        if !add.success() {
            return Err(add.into_error("worktree add"));
        }

        let record = self
            .verify_and_persist(
                spec,
                worktree_id,
                op_id,
                token,
                &facts.repository_root,
                &repo_identity,
                &base_oid,
            )
            .await?;
        Ok(WorktreeCreateOutcome::Created(record))
    }

    /// Post-`worktree add` steps, shared with crash-resume: write metadata,
    /// verify HEAD/branch, persist the record, complete the operation.
    #[allow(clippy::too_many_arguments)]
    async fn verify_and_persist(
        &self,
        spec: &WorktreeSpec,
        worktree_id: &str,
        op_id: &str,
        token: &str,
        repo_root: &Path,
        repo_identity: &str,
        base_oid: &str,
    ) -> Result<WorktreeRecord, CoreError> {
        // Symlink/rename escape guard on the realized path.
        let canonical = naming::canonicalize_for_git(&spec.worktree_path)
            .map_err(|e| ws_err(format!("canonicalize created worktree: {e}")))?;
        if !canonical.starts_with(&self.worktree_root) {
            return Err(ws_err(format!(
                "created worktree escaped the harness root: {}",
                canonical.display()
            )));
        }

        let record = WorktreeRecord {
            worktree_id: worktree_id.to_string(),
            project_id: spec.project_id.clone(),
            task_id: spec.task_id.clone(),
            execution_id: spec.execution_id.clone(),
            repository_root: repo_root.to_string_lossy().into_owned(),
            repository_identity: repo_identity.to_string(),
            worktree_path: canonical.to_string_lossy().into_owned(),
            branch_name: spec.branch_name.clone(),
            base_commit: base_oid.to_string(),
            owner_supervisor_id: spec.owner_supervisor_id.clone(),
            operation_id: op_id.to_string(),
            status: WorktreeStatus::Active,
            created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };

        // Ownership metadata BEFORE the DB record: a crash after `add` leaves
        // provable metadata for the reconciler.
        metadata::write_sidecar(&canonical, &WorktreeMetadata::from_record(&record))?;

        // Verify actual HEAD and branch — never trust the exit code alone.
        let head = self
            .inspector
            .git()
            .run_ok(&canonical, &["rev-parse", "HEAD"])
            .await?;
        if head.trim() != base_oid {
            return Err(ws_err(format!(
                "worktree HEAD mismatch: expected {base_oid}, got {head}"
            )));
        }
        let branch = self
            .inspector
            .git()
            .run_ok(&canonical, &["rev-parse", "--abbrev-ref", "HEAD"])
            .await?;
        if branch.trim() != spec.branch_name {
            return Err(ws_err(format!(
                "worktree branch mismatch: expected {}, got {branch}",
                spec.branch_name
            )));
        }

        insert_record(&self.pool, &record).await?;
        self.ops
            .complete_claimed_operation(
                op_id,
                token,
                &serde_json::json!({
                    "worktree_id": worktree_id,
                    "worktree_path": record.worktree_path,
                    "base_commit": base_oid,
                }),
            )
            .await?;
        Ok(record)
    }

    // ── Inspect ──────────────────────────────────────────────────

    pub async fn inspect_worktree(
        &self,
        record: &WorktreeRecord,
    ) -> Result<WorktreeInspection, CoreError> {
        let path = PathBuf::from(&record.worktree_path);
        let path_exists = path.exists();
        let repo_root = PathBuf::from(&record.repository_root);
        let common = PathBuf::from(&record.repository_identity);

        let mut inspection = WorktreeInspection {
            path_exists,
            belongs_to_repository: false,
            head_commit: None,
            head_equals_base: false,
            head_descends_from_base: false,
            branch: None,
            branch_matches: false,
            metadata_present: false,
            metadata_matches: false,
            dirty: None,
            locked: false,
            prunable: false,
            git_admin_missing: false,
            moved_or_deleted: false,
        };

        // Ownership metadata (sidecar survives even when the dir is gone).
        if let Ok(Some(meta)) = metadata::read_sidecar(&path) {
            inspection.metadata_present = true;
            inspection.metadata_matches = meta.matches_record(record);
        }

        // Registration state from git's own porcelain (never DB-only).
        if repo_root.exists() {
            if let Ok(entries) = self.inspector.list_worktrees(&repo_root).await {
                let entry = entries.iter().find(|e| {
                    (path_exists
                        && naming::canonicalize_for_git(&e.path).ok().as_deref()
                            == naming::canonicalize_for_git(&path).ok().as_deref())
                        || e.path == path
                });
                if let Some(e) = entry {
                    inspection.locked = e.locked;
                    inspection.prunable = e.prunable;
                    if !path_exists || e.prunable {
                        inspection.moved_or_deleted = true;
                    }
                } else if path_exists {
                    // Directory exists but git no longer registers it.
                    inspection.git_admin_missing = true;
                }
            }
        }

        if !path_exists {
            inspection.moved_or_deleted = true;
            return Ok(inspection);
        }

        inspection.belongs_to_repository = self
            .inspector
            .path_belongs_to_repository(&path, &common)
            .await?;
        if !inspection.belongs_to_repository {
            inspection.git_admin_missing = true;
            return Ok(inspection);
        }

        let head = self
            .inspector
            .git()
            .run(&path, &["rev-parse", "HEAD"])
            .await?;
        if head.success() {
            let oid = head.stdout.trim().to_string();
            inspection.head_equals_base = oid == record.base_commit;
            let anc = self
                .inspector
                .git()
                .run(
                    &path,
                    &["merge-base", "--is-ancestor", &record.base_commit, &oid],
                )
                .await?;
            inspection.head_descends_from_base = anc.success();
            inspection.head_commit = Some(oid);
        }

        let branch = self
            .inspector
            .git()
            .run(&path, &["rev-parse", "--abbrev-ref", "HEAD"])
            .await?;
        if branch.success() {
            let b = branch.stdout.trim().to_string();
            inspection.branch_matches = b == record.branch_name;
            inspection.branch = Some(b);
        }

        inspection.dirty = Some(self.inspector.is_dirty(&path).await?);
        Ok(inspection)
    }

    // ── Remove ───────────────────────────────────────────────────

    pub async fn remove_worktree(
        &self,
        worktree_id: &str,
        policy: WorktreeRemovePolicy,
    ) -> Result<WorktreeRemoveOutcome, CoreError> {
        let Some(record) = self.get_record(worktree_id).await? else {
            return Err(ws_err(format!("unknown worktree: {worktree_id}")));
        };
        if record.status == WorktreeStatus::Removed {
            return Ok(WorktreeRemoveOutcome::AlreadyRemoved);
        }

        let ikey = format!("wt-remove-{worktree_id}");
        let payload = serde_json::json!({ "worktree_id": worktree_id });
        let op_id = match self
            .ops
            .begin(&record.task_id, "worktree_remove", &payload, &ikey)
            .await
        {
            Ok(id) => id,
            Err(_) => {
                let Some((existing_op, status)) = find_op_by_ikey(&self.pool, &ikey).await? else {
                    return Err(ws_err(
                        "remove operation insert failed without existing key".into(),
                    ));
                };
                match status.as_str() {
                    "completed" => return Ok(WorktreeRemoveOutcome::AlreadyRemoved),
                    "failed" => {
                        return Err(ws_err(format!(
                        "previous remove operation {existing_op} failed; reconciliation required"
                    )))
                    }
                    _ => existing_op,
                }
            }
        };

        let Some(token) = self.ops.try_claim_operation(&op_id, 60).await? else {
            return Ok(WorktreeRemoveOutcome::InProgress);
        };

        let result = self.remove_claimed(&record, policy, &op_id, &token).await;
        match &result {
            // Refusals release the claim so a corrected retry can re-claim.
            Ok(WorktreeRemoveOutcome::RefusedDirty { .. })
            | Ok(WorktreeRemoveOutcome::RefusedOwnershipUnverified { .. }) => {
                let _ = self.ops.release_operation_claim(&op_id, &token).await;
            }
            Err(e) => {
                let _ = self
                    .ops
                    .fail_claimed_operation(&op_id, &token, &e.message)
                    .await;
            }
            _ => {}
        }
        result
    }

    async fn remove_claimed(
        &self,
        record: &WorktreeRecord,
        policy: WorktreeRemovePolicy,
        op_id: &str,
        token: &str,
    ) -> Result<WorktreeRemoveOutcome, CoreError> {
        let path = PathBuf::from(&record.worktree_path);
        let repo_root = PathBuf::from(&record.repository_root);

        let _repo_guard = self.locks.acquire(&record.repository_identity).await;

        // Never remove anything that is not provably ours.
        let sidecar = metadata::read_sidecar(&path)?;
        let Some(meta) = sidecar else {
            mark_reconciliation_required(&self.pool, &record.worktree_id).await?;
            return Ok(WorktreeRemoveOutcome::RefusedOwnershipUnverified {
                reason: "ownership metadata missing".into(),
            });
        };
        if !meta.matches_record(record) {
            mark_reconciliation_required(&self.pool, &record.worktree_id).await?;
            return Ok(WorktreeRemoveOutcome::RefusedOwnershipUnverified {
                reason: "ownership metadata does not match record".into(),
            });
        }

        // Safety guards: never the repository root, never outside our root.
        let repo_canonical = naming::canonicalize_for_git(&repo_root).ok();
        let path_canonical = naming::canonicalize_for_git(&path).ok();
        if path_canonical.is_some() && path_canonical == repo_canonical {
            return Err(ws_err("refusing to remove the repository root".into()));
        }
        if !PathBuf::from(&record.worktree_path).starts_with(&self.worktree_root) {
            return Err(ws_err(format!(
                "refusing to remove a path outside the harness worktree root: {}",
                record.worktree_path
            )));
        }

        // Directory already gone: prune administrative leftovers, finish.
        if !path.exists() {
            if repo_root.exists() {
                let listed = self.inspector.list_worktrees(&repo_root).await?;
                let registered = listed.iter().any(|e| e.path == path);
                if registered {
                    let prune = self
                        .inspector
                        .git()
                        .run(&repo_root, &["worktree", "prune"])
                        .await?;
                    if !prune.success() {
                        return Err(prune.into_error("worktree prune"));
                    }
                }
            }
            let _ = metadata::set_sidecar_state(&path, "removed");
            self.finish_remove(record, op_id, token).await?;
            return Ok(WorktreeRemoveOutcome::AlreadyRemoved);
        }

        // Dirty worktrees are refused unless the policy explicitly forces.
        let dirty_entries = self.inspector.dirty_entries(&path).await?;
        if !dirty_entries.is_empty() && !policy.force_dirty {
            return Ok(WorktreeRemoveOutcome::RefusedDirty {
                changed_entries: dirty_entries.len(),
            });
        }

        // Diagnostics + diff reference before destruction.
        let head = self
            .inspector
            .git()
            .run(&path, &["rev-parse", "HEAD"])
            .await
            .ok()
            .filter(|o| o.success())
            .map(|o| o.stdout.trim().to_string());
        metadata::write_removal_diagnostics(
            &path,
            &record.worktree_id,
            head.as_deref(),
            &dirty_entries,
        )?;
        metadata::set_sidecar_state(&path, "removing")?;

        // Side effect. `--force` is an explicit policy decision, never default.
        let path_str = path.to_string_lossy().into_owned();
        let mut args = vec!["worktree", "remove"];
        if policy.force_dirty {
            args.push("--force");
        }
        args.push(&path_str);
        let removed = self.inspector.git().run(&repo_root, &args).await?;
        if !removed.success() {
            mark_reconciliation_required(&self.pool, &record.worktree_id).await?;
            return Err(removed.into_error("worktree remove"));
        }

        let _ = metadata::set_sidecar_state(&path, "removed");
        self.finish_remove(record, op_id, token).await?;
        Ok(WorktreeRemoveOutcome::Removed)
    }

    async fn finish_remove(
        &self,
        record: &WorktreeRecord,
        op_id: &str,
        token: &str,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE worktrees SET status='removed', removed_at=datetime('now'), updated_at=datetime('now'), version=version+1 WHERE id=?",
        )
        .bind(&record.worktree_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        self.ops
            .complete_claimed_operation(
                op_id,
                token,
                &serde_json::json!({ "worktree_id": record.worktree_id, "removed": true }),
            )
            .await?;
        Ok(())
    }

    // ── Records ──────────────────────────────────────────────────

    pub async fn get_record(&self, worktree_id: &str) -> Result<Option<WorktreeRecord>, CoreError> {
        get_record(&self.pool, worktree_id).await
    }
}

// ── Shared DB helpers (also used by the reconciler) ──────────────

pub(crate) async fn get_record(
    pool: &SqlitePool,
    worktree_id: &str,
) -> Result<Option<WorktreeRecord>, CoreError> {
    let row: Option<WtRow> = sqlx::query_as(
        "SELECT id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status, created_at FROM worktrees WHERE id = ?",
    )
    .bind(worktree_id)
    .fetch_optional(pool)
    .await
    .map_err(db_err)?;
    Ok(row.map(WtRow::into_record))
}

pub(crate) async fn list_records(
    pool: &SqlitePool,
    include_removed: bool,
) -> Result<Vec<WorktreeRecord>, CoreError> {
    let sql = if include_removed {
        "SELECT id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status, created_at FROM worktrees"
    } else {
        "SELECT id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status, created_at FROM worktrees WHERE status != 'removed'"
    };
    let rows: Vec<WtRow> = sqlx::query_as(sql).fetch_all(pool).await.map_err(db_err)?;
    Ok(rows.into_iter().map(WtRow::into_record).collect())
}

pub(crate) async fn insert_record(
    pool: &SqlitePool,
    record: &WorktreeRecord,
) -> Result<(), CoreError> {
    // Idempotent on resume: the id is deterministic per task/execution.
    sqlx::query(
        "INSERT OR IGNORE INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
    )
    .bind(&record.worktree_id)
    .bind(&record.project_id)
    .bind(&record.task_id)
    .bind(&record.execution_id)
    .bind(&record.repository_root)
    .bind(&record.repository_identity)
    .bind(&record.worktree_path)
    .bind(&record.branch_name)
    .bind(&record.base_commit)
    .bind(&record.owner_supervisor_id)
    .bind(&record.operation_id)
    .bind(record.status.as_str())
    .bind(&record.created_at)
    .execute(pool)
    .await
    .map_err(db_err)?;
    Ok(())
}

pub(crate) async fn mark_reconciliation_required(
    pool: &SqlitePool,
    worktree_id: &str,
) -> Result<(), CoreError> {
    sqlx::query(
        "UPDATE worktrees SET status='reconciliation_required', updated_at=datetime('now'), version=version+1 WHERE id=? AND status != 'removed'",
    )
    .bind(worktree_id)
    .execute(pool)
    .await
    .map_err(db_err)?;
    Ok(())
}

pub(crate) async fn find_op_by_ikey(
    pool: &SqlitePool,
    ikey: &str,
) -> Result<Option<(String, String)>, CoreError> {
    sqlx::query_as("SELECT operation_id, status FROM operations WHERE idempotency_key = ?")
        .bind(ikey)
        .fetch_optional(pool)
        .await
        .map_err(db_err)
}

#[derive(sqlx::FromRow)]
struct WtRow {
    id: String,
    project_id: String,
    task_id: String,
    execution_id: String,
    repository_root: String,
    repository_identity: String,
    worktree_path: String,
    branch_name: String,
    base_commit: String,
    owner_supervisor_id: String,
    operation_id: String,
    status: String,
    created_at: String,
}

impl WtRow {
    fn into_record(self) -> WorktreeRecord {
        WorktreeRecord {
            worktree_id: self.id,
            project_id: self.project_id,
            task_id: self.task_id,
            execution_id: self.execution_id,
            repository_root: self.repository_root,
            repository_identity: self.repository_identity,
            worktree_path: self.worktree_path,
            branch_name: self.branch_name,
            base_commit: self.base_commit,
            owner_supervisor_id: self.owner_supervisor_id,
            operation_id: self.operation_id,
            status: WorktreeStatus::parse(&self.status),
            created_at: self.created_at,
        }
    }
}

fn ws_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::new(
        ErrorCode::PersistenceError,
        e.to_string(),
        ErrorSource::System,
    )
}
