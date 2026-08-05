//! Minimal test to verify projects/tasks INSERT OR IGNORE behavior
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = std::env::temp_dir().join("fk-test-db");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp)?;
    let db_path = tmp.join("test.db");
    let test_repo = tmp.join("repo");
    std::fs::create_dir_all(&test_repo)?;
    let wt = std::env::temp_dir().join("fk-test-wt");

    let db = harness_runtime::db::Database::open(&db_path).await?;
    let rc = Arc::new(harness_runtime::liveness::RunContext::create(
        &tmp, "test", true,
    )?);
    let _graph = harness_runtime::production_graph::ProductionGraph::build(
        db.pool.clone(),
        &wt,
        &test_repo,
        rc,
    )?;

    // Now try inserting into projects with various approaches
    let project_id = "g-test-001";
    let task_id = "goal-g-test-001-task-1";
    let objective = "Test objective";

    // Test 1: basic INSERT OR IGNORE with bind
    let r1 = sqlx::query(
        "INSERT OR IGNORE INTO projects (id, objective, lifecycle) VALUES (?1, ?2, 'active')",
    )
    .bind(project_id)
    .bind(objective)
    .execute(&db.pool)
    .await;
    eprintln!(
        "Test1 (OR IGNORE, numbered): {:?}",
        r1.as_ref().map(|r| r.rows_affected())
    );

    // Test 2: basic INSERT with bind
    let project_id2 = "g-test-002";
    let r2 =
        sqlx::query("INSERT INTO projects (id, objective, lifecycle) VALUES (?1, ?2, 'active')")
            .bind(project_id2)
            .bind(objective)
            .execute(&db.pool)
            .await;
    eprintln!(
        "Test2 (plain INSERT, numbered): {:?}",
        r2.as_ref().map(|r| r.rows_affected())
    );

    // Test 3: OR IGNORE with positional ?
    let project_id3 = "g-test-003";
    let r3 = sqlx::query(
        "INSERT OR IGNORE INTO projects (id, objective, lifecycle) VALUES (?, ?, 'active')",
    )
    .bind(project_id3)
    .bind(objective)
    .execute(&db.pool)
    .await;
    eprintln!(
        "Test3 (OR IGNORE, positional): {:?}",
        r3.as_ref().map(|r| r.rows_affected())
    );

    // Check if project exists
    let proj_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM projects WHERE id = ?")
        .bind(project_id)
        .fetch_one(&db.pool)
        .await?;
    eprintln!("project exists: {}", proj_count > 0);

    // Insert task
    let r2 = sqlx::query(
        "INSERT OR IGNORE INTO tasks (id, project_id, goal, lifecycle) VALUES (?, ?, ?, 'submitted')",
    )
    .bind(task_id)
    .bind(project_id)
    .bind(objective)
    .execute(&db.pool)
    .await;
    eprintln!("tasks insert: {:?}", r2.as_ref().map(|r| r.rows_affected()));

    // Check if task exists
    let task_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE id = ?")
        .bind(task_id)
        .fetch_one(&db.pool)
        .await?;
    eprintln!("task exists: {}", task_count > 0);

    // Try with a simpler objective
    let task_id2 = "goal-g-test-001-task-2";
    let r3 = sqlx::query(
        "INSERT OR IGNORE INTO tasks (id, project_id, goal, lifecycle) VALUES (?, ?, ?, 'submitted')",
    )
    .bind(task_id2)
    .bind(project_id)
    .bind("simple")
    .execute(&db.pool)
    .await;
    eprintln!(
        "tasks insert (simple): {:?}",
        r3.as_ref().map(|r| r.rows_affected())
    );

    let task_count2: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE id = ?")
        .bind(task_id2)
        .fetch_one(&db.pool)
        .await?;
    eprintln!("task2 exists: {}", task_count2 > 0);

    // Check schema
    let schema: Vec<(String,)> =
        sqlx::query_as("SELECT sql FROM sqlite_master WHERE type='table' AND name='projects'")
            .fetch_all(&db.pool)
            .await?;
    for (sql,) in &schema {
        eprintln!("projects schema: {}", sql);
    }

    // Try raw INSERT (not OR IGNORE)
    let r4 = sqlx::query(
        "INSERT INTO projects (id, objective, lifecycle) VALUES ('g-raw-001', 'test', 'active')",
    )
    .execute(&db.pool)
    .await;
    eprintln!("raw INSERT: {:?}", r4.as_ref().map(|r| r.rows_affected()));

    // Check if raw insert worked
    let proj_count2: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM projects WHERE id = 'g-raw-001'")
            .fetch_one(&db.pool)
            .await?;
    eprintln!("raw project exists: {}", proj_count2 > 0);

    // List all projects
    let projects: Vec<(String, String)> = sqlx::query_as("SELECT id, objective FROM projects")
        .fetch_all(&db.pool)
        .await?;
    eprintln!("projects: {:?}", projects);

    drop(db);
    Ok(())
}
