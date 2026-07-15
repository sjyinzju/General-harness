//! WorktreeReconciler — cross-checks SQLite records, ownership metadata,
//! the filesystem, `git worktree list --porcelain`, and unfinished
//! Operations, producing structured drift diagnostics.
//!
//! Repairs are limited to what is safe and deterministic:
//! - persist a DB record that metadata + git provably support;
//! - complete Operations whose side effect verifiably succeeded;
//! - mark records `reconciliation_required`;
//! - delete stale, empty, harness-owned temp entries.
//!
//! It NEVER deletes dirty, unknown, or ownership-unprovable worktrees.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

use super::inspector::{RepositoryInspector, WorktreeListEntry};
use super::manager::{get_record, insert_record, list_records, mark_reconciliation_required};
use super::metadata::{self, WorktreeMetadata, SIDECAR_SUFFIX};
use super::types::{WorktreeRecord, WorktreeSpec, WorktreeStatus};
use crate::operation::OperationManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorktreeDriftKind {
    DbPresentFsMissing,
    FsPresentDbMissing,
    GitPresentMetadataMissing,
    MetadataPresentGitMissing,
    OwnerMismatch,
    PathMismatch,
    BranchMismatch,
    BaseMismatch,
    DirtyOrphan,
    StaleTempDirectory,
    IncompleteCreateOperation,
    IncompleteRemoveOperation,
}

#[derive(Debug, Clone)]
pub struct WorktreeDrift {
    pub kind: WorktreeDriftKind,
    pub worktree_id: Option<String>,
    pub path: Option<PathBuf>,
    pub detail: String,
    /// True when a safe deterministic repair was applied.
    pub repaired: bool,
    pub repair: Option<String>,
}

pub struct WorktreeReconciler {
    pool: SqlitePool,
    inspector: RepositoryInspector,
    ops: OperationManager,
    worktree_root: PathBuf,
    supervisor_id: String,
}

impl WorktreeReconciler {
    pub fn new(
        pool: SqlitePool,
        inspector: RepositoryInspector,
        worktree_root: PathBuf,
        supervisor_id: String,
    ) -> Self {
        Self {
            ops: OperationManager::new(pool.clone()),
            pool,
            inspector,
            worktree_root,
            supervisor_id,
        }
    }

    pub async fn reconcile(&self) -> Result<Vec<WorktreeDrift>, CoreError> {
        let mut drifts = Vec::new();

        let records = list_records(&self.pool, false).await?;
        let sidecars = scan_sidecars(&self.worktree_root);
        let git_lists = self.collect_git_lists(&records).await;
        let pending_ops = self.pending_worktree_ops().await?;

        // ── Records vs FS / metadata / git ───────────────────────
        for record in &records {
            let path = PathBuf::from(&record.worktree_path);
            let listed = git_lists
                .get(&record.repository_identity)
                .map(|entries| entries.iter().any(|e| paths_equal(&e.path, &path)))
                .unwrap_or(false);

            if !path.exists() {
                mark_reconciliation_required(&self.pool, &record.worktree_id).await?;
                drifts.push(drift(
                    WorktreeDriftKind::DbPresentFsMissing,
                    Some(&record.worktree_id),
                    Some(&path),
                    "record exists but the worktree directory is missing",
                    true,
                    Some("marked reconciliation_required"),
                ));
                continue;
            }

            let sidecar = metadata::read_sidecar(&path).ok().flatten();
            match &sidecar {
                None => {
                    mark_reconciliation_required(&self.pool, &record.worktree_id).await?;
                    drifts.push(drift(
                        WorktreeDriftKind::GitPresentMetadataMissing,
                        Some(&record.worktree_id),
                        Some(&path),
                        "worktree present but ownership metadata is missing",
                        true,
                        Some("marked reconciliation_required"),
                    ));
                }
                Some(meta) => {
                    if meta.owner_supervisor_id != record.owner_supervisor_id {
                        drifts.push(drift(
                            WorktreeDriftKind::OwnerMismatch,
                            Some(&record.worktree_id),
                            Some(&path),
                            &format!(
                                "metadata owner {} != record owner {} (no automatic action)",
                                meta.owner_supervisor_id, record.owner_supervisor_id
                            ),
                            false,
                            None,
                        ));
                    }
                    if meta.worktree_path != record.worktree_path {
                        drifts.push(drift(
                            WorktreeDriftKind::PathMismatch,
                            Some(&record.worktree_id),
                            Some(&path),
                            &format!("metadata path {} != record path", meta.worktree_path),
                            false,
                            None,
                        ));
                    }
                    if meta.branch != record.branch_name {
                        drifts.push(drift(
                            WorktreeDriftKind::BranchMismatch,
                            Some(&record.worktree_id),
                            Some(&path),
                            &format!(
                                "metadata branch {} != record branch {}",
                                meta.branch, record.branch_name
                            ),
                            false,
                            None,
                        ));
                    }
                    if meta.base_commit != record.base_commit {
                        drifts.push(drift(
                            WorktreeDriftKind::BaseMismatch,
                            Some(&record.worktree_id),
                            Some(&path),
                            &format!(
                                "metadata base {} != record base {}",
                                meta.base_commit, record.base_commit
                            ),
                            false,
                            None,
                        ));
                    }
                }
            }

            if !listed {
                drifts.push(drift(
                    WorktreeDriftKind::MetadataPresentGitMissing,
                    Some(&record.worktree_id),
                    Some(&path),
                    "record/metadata present but git does not register this worktree",
                    false,
                    None,
                ));
            }
        }

        // ── Sidecars without records ─────────────────────────────
        for (path, meta) in &sidecars {
            if meta.state == "removed" {
                continue;
            }
            if get_record(&self.pool, &meta.worktree_id).await?.is_some() {
                continue;
            }
            drifts.push(drift(
                WorktreeDriftKind::FsPresentDbMissing,
                Some(&meta.worktree_id),
                Some(path),
                "ownership metadata exists on disk but no DB record",
                false,
                None,
            ));
            if meta.owner_supervisor_id != self.supervisor_id {
                if let Ok(true) = self.is_dirty_if_exists(path).await {
                    drifts.push(drift(
                        WorktreeDriftKind::DirtyOrphan,
                        Some(&meta.worktree_id),
                        Some(path),
                        "orphan worktree of another supervisor has uncommitted changes (never auto-deleted)",
                        false,
                        None,
                    ));
                }
            }
        }

        // ── Unfinished operations ────────────────────────────────
        for op in &pending_ops {
            match op.op_type.as_str() {
                "worktree_create" => {
                    let repaired = self.repair_incomplete_create(op, &mut drifts).await?;
                    if !repaired {
                        drifts.push(drift(
                            WorktreeDriftKind::IncompleteCreateOperation,
                            op.worktree_id.as_deref(),
                            None,
                            &format!(
                                "create operation {} unfinished; side effect not provable — left for manual/owner resolution",
                                op.operation_id
                            ),
                            false,
                            None,
                        ));
                    }
                }
                "worktree_remove" => {
                    let repaired = self.repair_incomplete_remove(op, &mut drifts).await?;
                    if !repaired {
                        drifts.push(drift(
                            WorktreeDriftKind::IncompleteRemoveOperation,
                            op.worktree_id.as_deref(),
                            None,
                            &format!(
                                "remove operation {} unfinished; worktree still present or unprovable",
                                op.operation_id
                            ),
                            false,
                            None,
                        ));
                    }
                }
                _ => {}
            }
        }

        // ── Stale harness temp entries (safe, deterministic cleanup) ──
        for stale in scan_stale_temp(&self.worktree_root) {
            let removed = if stale.is_dir() {
                std::fs::remove_dir(&stale).is_ok() // refuses non-empty dirs
            } else {
                std::fs::remove_file(&stale).is_ok()
            };
            drifts.push(drift(
                WorktreeDriftKind::StaleTempDirectory,
                None,
                Some(&stale),
                "stale harness temp entry",
                removed,
                removed.then_some("deleted"),
            ));
        }

        Ok(drifts)
    }

    /// Create op crashed before completion. Provable success = git registers
    /// the path AND our sidecar matches the worktree id. Repair: persist the
    /// record and complete the operation.
    async fn repair_incomplete_create(
        &self,
        op: &PendingOp,
        drifts: &mut Vec<WorktreeDrift>,
    ) -> Result<bool, CoreError> {
        let Some(spec) = op.spec.as_ref() else {
            return Ok(false);
        };
        let Some(worktree_id) = op.worktree_id.as_deref() else {
            return Ok(false);
        };
        let path = spec.worktree_path.clone();
        if !path.exists() {
            return Ok(false);
        }
        let Ok(Some(meta)) = metadata::read_sidecar(&path) else {
            return Ok(false);
        };
        if meta.worktree_id != worktree_id {
            return Ok(false);
        }
        // Confirm with git itself.
        let repo_root = spec.repository_root.clone();
        if !repo_root.exists() {
            return Ok(false);
        }
        let listed = self
            .inspector
            .list_worktrees(&repo_root)
            .await?
            .iter()
            .any(|e| paths_equal(&e.path, &path));
        if !listed {
            return Ok(false);
        }

        // Claim → persist → complete.
        let Some(token) = self.ops.try_claim_operation(&op.operation_id, 60).await? else {
            return Ok(false); // live owner still holds it
        };
        let record = WorktreeRecord {
            worktree_id: worktree_id.to_string(),
            project_id: meta.project_id.clone(),
            task_id: meta.task_id.clone(),
            execution_id: meta.execution_id.clone(),
            repository_root: repo_root
                .canonicalize()
                .unwrap_or(repo_root)
                .to_string_lossy()
                .into_owned(),
            repository_identity: meta.repository_identity.clone(),
            worktree_path: meta.worktree_path.clone(),
            branch_name: meta.branch.clone(),
            base_commit: meta.base_commit.clone(),
            owner_supervisor_id: meta.owner_supervisor_id.clone(),
            operation_id: op.operation_id.clone(),
            status: WorktreeStatus::Active,
            created_at: meta.created_at.clone(),
        };
        insert_record(&self.pool, &record).await?;
        self.ops
            .complete_claimed_operation(
                &op.operation_id,
                &token,
                &serde_json::json!({ "worktree_id": worktree_id, "reconciled_by": self.supervisor_id }),
            )
            .await?;
        drifts.push(drift(
            WorktreeDriftKind::IncompleteCreateOperation,
            Some(worktree_id),
            Some(&path),
            "create succeeded externally before crash",
            true,
            Some("record persisted and operation completed"),
        ));
        Ok(true)
    }

    /// Remove op crashed before completion. Provable success = directory gone
    /// AND git no longer registers the path. Repair: mark record removed and
    /// complete the operation.
    async fn repair_incomplete_remove(
        &self,
        op: &PendingOp,
        drifts: &mut Vec<WorktreeDrift>,
    ) -> Result<bool, CoreError> {
        let Some(worktree_id) = op.worktree_id.as_deref() else {
            return Ok(false);
        };
        let Some(record) = get_record(&self.pool, worktree_id).await? else {
            return Ok(false);
        };
        if record.status == WorktreeStatus::Removed {
            // DB already final — just complete the op.
            let Some(token) = self.ops.try_claim_operation(&op.operation_id, 60).await? else {
                return Ok(false);
            };
            self.ops
                .complete_claimed_operation(
                    &op.operation_id,
                    &token,
                    &serde_json::json!({ "worktree_id": worktree_id, "reconciled_by": self.supervisor_id }),
                )
                .await?;
            return Ok(true);
        }
        let path = PathBuf::from(&record.worktree_path);
        if path.exists() {
            return Ok(false);
        }
        let repo_root = PathBuf::from(&record.repository_root);
        if repo_root.exists() {
            let still_listed = self
                .inspector
                .list_worktrees(&repo_root)
                .await?
                .iter()
                .any(|e| paths_equal(&e.path, &path));
            if still_listed {
                return Ok(false);
            }
        }
        let Some(token) = self.ops.try_claim_operation(&op.operation_id, 60).await? else {
            return Ok(false);
        };
        sqlx::query(
            "UPDATE worktrees SET status='removed', removed_at=datetime('now'), updated_at=datetime('now'), version=version+1 WHERE id=?",
        )
        .bind(worktree_id)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        self.ops
            .complete_claimed_operation(
                &op.operation_id,
                &token,
                &serde_json::json!({ "worktree_id": worktree_id, "reconciled_by": self.supervisor_id }),
            )
            .await?;
        drifts.push(drift(
            WorktreeDriftKind::IncompleteRemoveOperation,
            Some(worktree_id),
            Some(&path),
            "remove succeeded externally before crash",
            true,
            Some("record marked removed and operation completed"),
        ));
        Ok(true)
    }

    async fn collect_git_lists(
        &self,
        records: &[WorktreeRecord],
    ) -> HashMap<String, Vec<WorktreeListEntry>> {
        let mut map = HashMap::new();
        for record in records {
            if map.contains_key(&record.repository_identity) {
                continue;
            }
            let root = PathBuf::from(&record.repository_root);
            if !root.exists() {
                continue;
            }
            if let Ok(entries) = self.inspector.list_worktrees(&root).await {
                map.insert(record.repository_identity.clone(), entries);
            }
        }
        map
    }

    async fn is_dirty_if_exists(&self, path: &Path) -> Result<bool, CoreError> {
        if !path.exists() {
            return Ok(false);
        }
        self.inspector.is_dirty(path).await
    }

    async fn pending_worktree_ops(&self) -> Result<Vec<PendingOp>, CoreError> {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT operation_id, operation_type, status, payload_json FROM operations WHERE operation_type IN ('worktree_create','worktree_remove') AND status IN ('pending','running','reconciliation_required')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(rows
            .into_iter()
            .map(|(operation_id, op_type, status, payload)| {
                let json: serde_json::Value =
                    serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null);
                let worktree_id = json["worktree_id"].as_str().map(str::to_string);
                let spec: Option<WorktreeSpec> = serde_json::from_value(json["spec"].clone()).ok();
                PendingOp {
                    operation_id,
                    op_type,
                    _status: status,
                    worktree_id,
                    spec,
                }
            })
            .collect())
    }
}

struct PendingOp {
    operation_id: String,
    op_type: String,
    _status: String,
    worktree_id: Option<String>,
    spec: Option<WorktreeSpec>,
}

fn drift(
    kind: WorktreeDriftKind,
    worktree_id: Option<&str>,
    path: Option<&Path>,
    detail: &str,
    repaired: bool,
    repair: Option<&str>,
) -> WorktreeDrift {
    WorktreeDrift {
        kind,
        worktree_id: worktree_id.map(str::to_string),
        path: path.map(Path::to_path_buf),
        detail: detail.to_string(),
        repaired,
        repair: repair.map(str::to_string),
    }
}

fn paths_equal(a: &Path, b: &Path) -> bool {
    match (
        super::naming::canonicalize_for_git(a),
        super::naming::canonicalize_for_git(b),
    ) {
        (Ok(ca), Ok(cb)) => ca == cb,
        _ => a == b,
    }
}

/// All `*.harness.json` sidecars under the worktree root (bounded depth).
fn scan_sidecars(root: &Path) -> Vec<(PathBuf, WorktreeMetadata)> {
    let mut out = Vec::new();
    walk(root, 0, &mut |file| {
        let Some(name) = file.file_name().and_then(|n| n.to_str()) else {
            return;
        };
        if let Some(dir_name) = name.strip_suffix(SIDECAR_SUFFIX) {
            let worktree_path = file.with_file_name(dir_name);
            if let Ok(raw) = std::fs::read_to_string(file) {
                if let Ok(meta) = serde_json::from_str::<WorktreeMetadata>(&raw) {
                    out.push((worktree_path, meta));
                }
            }
        }
    });
    out
}

/// Harness temp remnants: `*.harness.json.tmp` files and empty `.wt-tmp-*`
/// directories.
fn scan_stale_temp(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk(root, 0, &mut |file| {
        let Some(name) = file.file_name().and_then(|n| n.to_str()) else {
            return;
        };
        if name.ends_with(".harness.json.tmp") {
            out.push(file.to_path_buf());
        }
    });
    walk_dirs(root, 0, &mut |dir| {
        let Some(name) = dir.file_name().and_then(|n| n.to_str()) else {
            return;
        };
        if name.starts_with(".wt-tmp-")
            && std::fs::read_dir(dir)
                .map(|mut d| d.next().is_none())
                .unwrap_or(false)
        {
            out.push(dir.to_path_buf());
        }
    });
    out
}

fn walk(dir: &Path, depth: usize, f: &mut impl FnMut(&Path)) {
    if depth > 3 {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            walk(&p, depth + 1, f);
        } else {
            f(&p);
        }
    }
}

fn walk_dirs(dir: &Path, depth: usize, f: &mut impl FnMut(&Path)) {
    if depth > 3 {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            f(&p);
            walk_dirs(&p, depth + 1, f);
        }
    }
}
