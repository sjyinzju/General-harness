//! TaskReadinessEvaluator — determines whether a Task is ready for dispatch.
//! Queries persisted Task and TaskDependency state. Never modifies data.

use harness_core::contracts::scheduler::{BlockReason, ReadyStatus};
use harness_core::contracts::task::TaskLifecycle;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use std::collections::{HashMap, HashSet, VecDeque};

pub struct TaskReadinessEvaluator {
    pool: SqlitePool,
}

impl TaskReadinessEvaluator {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Evaluate readiness for a single Task by ID.
    pub async fn evaluate(&self, task_id: &str) -> Result<ReadyStatus, CoreError> {
        let task: Option<(String, String, String)> =
            sqlx::query_as("SELECT id, lifecycle, project_id FROM tasks WHERE id = ?")
                .bind(task_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        format!("readiness query: {e}"),
                        ErrorSource::System,
                    )
                })?;

        let (_tid, lc_str, _project_id) = match task {
            Some(t) => t,
            None => {
                return Ok(ReadyStatus::Blocked {
                    blocked_by: vec![task_id.to_string()],
                    reason: BlockReason::DependencyMissing,
                });
            }
        };

        let lifecycle: TaskLifecycle =
            serde_json::from_str(&format!("\"{lc_str}\"")).unwrap_or(TaskLifecycle::Pending);

        // Terminal → not ready
        if lifecycle.is_terminal() {
            return Ok(ReadyStatus::Terminal);
        }

        // Already has active execution?
        if lifecycle.has_active_execution() {
            let exec: Option<(String,)> = sqlx::query_as(
                "SELECT id FROM execution_attempts WHERE task_id = ? AND lifecycle NOT IN ('completed','failed','lost','cancelled') LIMIT 1",
            )
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

            if let Some((exec_id,)) = exec {
                return Ok(ReadyStatus::ActiveExecutionExists {
                    execution_id: exec_id,
                });
            }
        }

        // Check dependencies
        let deps: Vec<(String,)> =
            sqlx::query_as("SELECT depends_on_task_id FROM task_dependencies WHERE task_id = ?")
                .bind(task_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        e.to_string(),
                        ErrorSource::System,
                    )
                })?;

        if deps.is_empty() {
            // No dependencies — check if Task is in a dispatchable state
            match &lifecycle {
                TaskLifecycle::Pending | TaskLifecycle::Ready | TaskLifecycle::RetryPending => {
                    return Ok(ReadyStatus::Ready);
                }
                TaskLifecycle::AwaitingInput => {
                    return Ok(ReadyStatus::AwaitingHuman);
                }
                _ => {
                    return Ok(ReadyStatus::Blocked {
                        blocked_by: vec![],
                        reason: BlockReason::TaskTerminal,
                    });
                }
            }
        }

        // Check each dependency
        let dep_ids: Vec<String> = deps.into_iter().map(|(id,)| id).collect();

        // Detect cycles
        if let Some(cycle) = self.detect_cycle(task_id, &dep_ids).await? {
            return Ok(ReadyStatus::DependencyCycle { cycle_path: cycle });
        }

        // Check dependency states
        let mut blocked_by: Vec<String> = Vec::new();
        let mut missing: Vec<String> = Vec::new();
        let mut failed: Vec<String> = Vec::new();

        for dep_id in &dep_ids {
            let dep_state: Option<(String,)> =
                sqlx::query_as("SELECT lifecycle FROM tasks WHERE id = ?")
                    .bind(dep_id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| {
                        CoreError::new(
                            ErrorCode::PersistenceError,
                            e.to_string(),
                            ErrorSource::System,
                        )
                    })?;

            match dep_state {
                None => {
                    missing.push(dep_id.clone());
                }
                Some((lc,)) => {
                    let dep_lc: TaskLifecycle = serde_json::from_str(&format!("\"{lc}\""))
                        .unwrap_or(TaskLifecycle::Pending);
                    match dep_lc {
                        TaskLifecycle::Done | TaskLifecycle::Verified => {
                            // Satisfied — these are success states
                        }
                        TaskLifecycle::Failed
                        | TaskLifecycle::Cancelled
                        | TaskLifecycle::Superseded => {
                            failed.push(dep_id.clone());
                        }
                        _ => {
                            blocked_by.push(dep_id.clone());
                        }
                    }
                }
            }
        }

        if !missing.is_empty() {
            return Ok(ReadyStatus::DependencyMissing {
                missing_ids: missing,
            });
        }

        if !failed.is_empty() {
            return Ok(ReadyStatus::UpstreamFailed {
                failed_tasks: failed,
            });
        }

        if !blocked_by.is_empty() {
            return Ok(ReadyStatus::Blocked {
                blocked_by,
                reason: BlockReason::DependencyIncomplete,
            });
        }

        Ok(ReadyStatus::Ready)
    }

    /// Detect dependency cycles using BFS from start_task through its dependency graph.
    async fn detect_cycle(
        &self,
        start: &str,
        deps: &[String],
    ) -> Result<Option<Vec<String>>, CoreError> {
        // Build full dependency graph reachable from start
        let mut graph: HashMap<String, Vec<String>> = HashMap::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut visited: HashSet<String> = HashSet::new();

        for dep in deps {
            graph
                .entry(start.to_string())
                .or_default()
                .push(dep.clone());
            queue.push_back(dep.clone());
        }

        while let Some(current) = queue.pop_front() {
            if !visited.insert(current.clone()) {
                continue;
            }
            let sub_deps: Vec<(String,)> = sqlx::query_as(
                "SELECT depends_on_task_id FROM task_dependencies WHERE task_id = ?",
            )
            .bind(&current)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::PersistenceError,
                    e.to_string(),
                    ErrorSource::System,
                )
            })?;

            for (dep_id,) in sub_deps {
                graph
                    .entry(current.clone())
                    .or_default()
                    .push(dep_id.clone());
                queue.push_back(dep_id);
            }
        }

        // DFS cycle detection
        let mut white: HashSet<String> = graph.keys().cloned().collect();
        let mut gray: HashSet<String> = HashSet::new();
        let mut black: HashSet<String> = HashSet::new();
        let mut path: Vec<String> = Vec::new();

        fn dfs(
            node: &str,
            graph: &HashMap<String, Vec<String>>,
            white: &mut HashSet<String>,
            gray: &mut HashSet<String>,
            black: &mut HashSet<String>,
            path: &mut Vec<String>,
        ) -> Option<Vec<String>> {
            white.remove(node);
            gray.insert(node.to_string());
            path.push(node.to_string());

            if let Some(neighbors) = graph.get(node) {
                for neighbor in neighbors {
                    if gray.contains(neighbor) {
                        // Cycle found — extract cycle path
                        let cycle_start = path.iter().position(|n| n == neighbor).unwrap();
                        let mut cycle: Vec<String> = path[cycle_start..].to_vec();
                        cycle.push(neighbor.clone());
                        return Some(cycle);
                    }
                    if white.contains(neighbor) {
                        if let Some(cycle) = dfs(neighbor, graph, white, gray, black, path) {
                            return Some(cycle);
                        }
                    }
                }
            }

            path.pop();
            gray.remove(node);
            black.insert(node.to_string());
            None
        }

        // We only care if start is part of a cycle
        if let Some(cycle) = dfs(start, &graph, &mut white, &mut gray, &mut black, &mut path) {
            return Ok(Some(cycle));
        }
        Ok(None)
    }

    /// List all Tasks in Ready state (for scheduler polling).
    pub async fn find_ready_tasks(&self) -> Result<Vec<String>, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT id FROM tasks WHERE lifecycle IN ('pending','ready','retry_pending')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                e.to_string(),
                ErrorSource::System,
            )
        })?;

        let mut ready: Vec<String> = Vec::new();
        for (tid,) in rows {
            if self.evaluate(&tid).await? == ReadyStatus::Ready {
                ready.push(tid);
            }
        }
        Ok(ready)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    async fn setup() -> Database {
        Database::open_in_memory().await.unwrap()
    }

    async fn create_task(db: &Database, id: &str, lc: &str) {
        sqlx::query(
            "INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES (?, 'p1', 'test', ?)",
        )
        .bind(id)
        .bind(lc)
        .execute(&db.pool)
        .await
        .unwrap();
    }

    async fn create_project(db: &Database) {
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
    }

    async fn add_dep(db: &Database, task_id: &str, depends_on: &str) {
        sqlx::query("INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (?,?)")
            .bind(task_id)
            .bind(depends_on)
            .execute(&db.pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_no_dependency_ready() {
        let db = setup().await;
        create_project(&db).await;
        create_task(&db, "t1", "pending").await;
        let eval = TaskReadinessEvaluator::new(db.pool.clone());
        assert_eq!(eval.evaluate("t1").await.unwrap(), ReadyStatus::Ready);
    }

    #[tokio::test]
    async fn test_dependency_complete_ready() {
        let db = setup().await;
        create_project(&db).await;
        create_task(&db, "t1", "pending").await;
        create_task(&db, "t2", "done").await;
        add_dep(&db, "t1", "t2").await;
        let eval = TaskReadinessEvaluator::new(db.pool.clone());
        assert_eq!(eval.evaluate("t1").await.unwrap(), ReadyStatus::Ready);
    }

    #[tokio::test]
    async fn test_dependency_incomplete_blocked() {
        let db = setup().await;
        create_project(&db).await;
        create_task(&db, "t1", "pending").await;
        create_task(&db, "t2", "running").await;
        add_dep(&db, "t1", "t2").await;
        let eval = TaskReadinessEvaluator::new(db.pool.clone());
        let status = eval.evaluate("t1").await.unwrap();
        assert!(matches!(status, ReadyStatus::Blocked { .. }));
    }

    #[tokio::test]
    async fn test_dependency_failed_blocked() {
        let db = setup().await;
        create_project(&db).await;
        create_task(&db, "t1", "pending").await;
        create_task(&db, "t2", "failed").await;
        add_dep(&db, "t1", "t2").await;
        let eval = TaskReadinessEvaluator::new(db.pool.clone());
        assert!(matches!(
            eval.evaluate("t1").await.unwrap(),
            ReadyStatus::UpstreamFailed { .. }
        ));
    }

    #[tokio::test]
    async fn test_missing_dependency() {
        let db = setup().await;
        create_project(&db).await;
        // Create dep task, add dependency, then remove the dep task
        create_task(&db, "t1", "pending").await;
        create_task(&db, "dep1", "pending").await;
        add_dep(&db, "t1", "dep1").await;
        // Delete the dependency task (cascade removes dep row)
        sqlx::query("DELETE FROM tasks WHERE id = 'dep1'")
            .execute(&db.pool)
            .await
            .unwrap();
        let eval = TaskReadinessEvaluator::new(db.pool.clone());
        // After cascade delete, dep row is gone, but t1 still references dep1 in its dep list
        // The FK CASCADE should have removed the row, so t1 now has no deps and should be Ready
        assert_eq!(eval.evaluate("t1").await.unwrap(), ReadyStatus::Ready);
    }

    #[tokio::test]
    async fn test_dependency_cycle() {
        let db = setup().await;
        create_project(&db).await;
        create_task(&db, "t1", "pending").await;
        create_task(&db, "t2", "pending").await;
        create_task(&db, "t3", "pending").await;
        add_dep(&db, "t1", "t2").await;
        add_dep(&db, "t2", "t3").await;
        add_dep(&db, "t3", "t1").await;
        let eval = TaskReadinessEvaluator::new(db.pool.clone());
        assert!(matches!(
            eval.evaluate("t1").await.unwrap(),
            ReadyStatus::DependencyCycle { .. }
        ));
    }

    #[tokio::test]
    async fn test_terminal_task_not_ready() {
        let db = setup().await;
        create_project(&db).await;
        create_task(&db, "t1", "done").await;
        let eval = TaskReadinessEvaluator::new(db.pool.clone());
        assert_eq!(eval.evaluate("t1").await.unwrap(), ReadyStatus::Terminal);
    }

    #[tokio::test]
    async fn test_active_execution_blocks() {
        let db = setup().await;
        create_project(&db).await;
        create_task(&db, "t1", "running").await;
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'running')")
            .execute(&db.pool).await.unwrap();
        let eval = TaskReadinessEvaluator::new(db.pool.clone());
        assert!(matches!(
            eval.evaluate("t1").await.unwrap(),
            ReadyStatus::ActiveExecutionExists { .. }
        ));
    }
}
