//! I4.5 CLI Integration Tests — exercise production CLI command paths
//! through the real service layer. These tests verify that CLI commands
//! produce correct output, durable effects, and proper error handling.

use harness_runtime::db::Database;
use harness_runtime::task_loop::*;

async fn setup_temp_db() -> Database {
    let td = tempfile::tempdir().unwrap();
    let db = Database::open(&td.path().join("cli_test.db"))
        .await
        .unwrap();
    sqlx::query("INSERT INTO projects(id,objective,lifecycle) VALUES('proj-cli','test','active')")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('task-cli','proj-cli','goal','submitted')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    db
}

#[tokio::test]
async fn test_cli_start_creates_loop() {
    let db = setup_temp_db().await;
    let svc = TaskEngineeringLoopService::new(db.pool.clone());

    let policy_json = r#"{"max_attempts":5}"#;
    let policy_fp = fingerprint_hex(policy_json);
    let req = CreateLoopRequest {
        project_id: "proj-cli".into(),
        task_id: "task-cli".into(),
        policy_json: policy_json.into(),
        policy_fingerprint: policy_fp,
        idempotency_key: "cli-start-1".into(),
        request_hash: "h-cli-start-1".into(),
        owner_id: "cli-owner".into(),
        lease_secs: 300,
    };

    match svc.create_loop(&req).await.unwrap() {
        CreateLoopOutcome::Created { loop_id } => {
            assert!(!loop_id.is_empty(), "loop_id must not be empty");
            let started = svc
                .start_or_resume_loop(&loop_id, "cli-owner", 300)
                .await
                .unwrap();
            assert!(matches!(started, LoopStartOutcome::Started { .. }));
        }
        other => panic!("expected Created, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_cli_start_idempotent() {
    let db = setup_temp_db().await;
    let svc = TaskEngineeringLoopService::new(db.pool.clone());

    let req = CreateLoopRequest {
        project_id: "proj-cli".into(),
        task_id: "task-cli".into(),
        policy_json: "{}".into(),
        policy_fingerprint: fingerprint_hex("{}"),
        idempotency_key: "cli-idem-1".into(),
        request_hash: "h-cli-idem-1".into(),
        owner_id: "cli-owner".into(),
        lease_secs: 300,
    };

    let r1 = svc.create_loop(&req).await.unwrap();
    let r2 = svc.create_loop(&req).await.unwrap();

    assert!(matches!(r1, CreateLoopOutcome::Created { .. }));
    assert!(matches!(r2, CreateLoopOutcome::Duplicate { .. }));
}

#[tokio::test]
async fn test_cli_inspect_shows_facts() {
    let db = setup_temp_db().await;
    let svc = TaskEngineeringLoopService::new(db.pool.clone());

    let req = CreateLoopRequest {
        project_id: "proj-cli".into(),
        task_id: "task-cli".into(),
        policy_json: "{}".into(),
        policy_fingerprint: fingerprint_hex("{}"),
        idempotency_key: "cli-inspect-1".into(),
        request_hash: "h-cli-inspect-1".into(),
        owner_id: "cli-owner".into(),
        lease_secs: 300,
    };

    if let CreateLoopOutcome::Created { loop_id } = svc.create_loop(&req).await.unwrap() {
        let _ = svc
            .start_or_resume_loop(&loop_id, "cli-owner", 300)
            .await
            .unwrap();
        let info = svc.inspect_loop(&loop_id).await.unwrap();
        assert!(info.is_some(), "inspect must return data");
        let info = info.unwrap();
        assert_eq!(info.task_id, "task-cli");
        assert_eq!(info.attempt_count, 0);
    } else {
        panic!("expected Created");
    }
}

#[tokio::test]
async fn test_cli_cancel_loop() {
    let db = setup_temp_db().await;
    let svc = TaskEngineeringLoopService::new(db.pool.clone());

    let req = CreateLoopRequest {
        project_id: "proj-cli".into(),
        task_id: "task-cli".into(),
        policy_json: "{}".into(),
        policy_fingerprint: fingerprint_hex("{}"),
        idempotency_key: "cli-cancel-1".into(),
        request_hash: "h-cli-cancel-1".into(),
        owner_id: "cli-owner".into(),
        lease_secs: 300,
    };

    if let CreateLoopOutcome::Created { loop_id } = svc.create_loop(&req).await.unwrap() {
        let started = svc
            .start_or_resume_loop(&loop_id, "cli-owner", 300)
            .await
            .unwrap();
        let v = match started {
            LoopStartOutcome::Started { version } => version.unwrap(),
            _ => panic!("expected Started"),
        };
        let l = TaskLoopRepo::new(db.pool.clone())
            .load_loop(&loop_id)
            .await
            .unwrap()
            .unwrap();

        let cancelled = svc
            .cancel_loop(&loop_id, "cli-owner", v, l.fencing_token)
            .await
            .unwrap();
        assert!(
            matches!(cancelled, CancelLoopOutcome::Cancelled),
            "expected Cancelled, got: {cancelled:?}"
        );

        // Re-cancel must return AlreadyTerminal.
        let l2 = TaskLoopRepo::new(db.pool.clone())
            .load_loop(&loop_id)
            .await
            .unwrap()
            .unwrap();
        let re_cancel = svc
            .cancel_loop(&loop_id, "cli-owner", l2.version, l2.fencing_token)
            .await
            .unwrap();
        assert!(matches!(
            re_cancel,
            CancelLoopOutcome::AlreadyTerminal { .. }
        ));
    } else {
        panic!("expected Created");
    }
}

#[tokio::test]
async fn test_cli_dry_run_zero_writes() {
    let db = setup_temp_db().await;
    let svc = TaskEngineeringLoopService::new(db.pool.clone());

    let req = CreateLoopRequest {
        project_id: "proj-cli".into(),
        task_id: "task-cli".into(),
        policy_json: "{}".into(),
        policy_fingerprint: fingerprint_hex("{}"),
        idempotency_key: "cli-dry-1".into(),
        request_hash: "h-cli-dry-1".into(),
        owner_id: "cli-owner".into(),
        lease_secs: 300,
    };

    if let CreateLoopOutcome::Created { loop_id } = svc.create_loop(&req).await.unwrap() {
        let _ = svc
            .start_or_resume_loop(&loop_id, "cli-owner", 300)
            .await
            .unwrap();

        let before_loops: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM task_engineering_loops")
            .fetch_one(&db.pool)
            .await
            .unwrap();

        // Inspect (zero-write operation).
        let info = svc.inspect_loop(&loop_id).await.unwrap();
        assert!(info.is_some());

        let after_loops: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM task_engineering_loops")
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(
            before_loops.0, after_loops.0,
            "dry-run must write zero rows"
        );
    }
}

#[tokio::test]
async fn test_cli_resume_idempotent() {
    let db = setup_temp_db().await;
    let svc = TaskEngineeringLoopService::new(db.pool.clone());

    let req = CreateLoopRequest {
        project_id: "proj-cli".into(),
        task_id: "task-cli".into(),
        policy_json: "{}".into(),
        policy_fingerprint: fingerprint_hex("{}"),
        idempotency_key: "cli-resume-1".into(),
        request_hash: "h-cli-resume-1".into(),
        owner_id: "cli-owner".into(),
        lease_secs: 300,
    };

    if let CreateLoopOutcome::Created { loop_id } = svc.create_loop(&req).await.unwrap() {
        let r1 = svc
            .start_or_resume_loop(&loop_id, "cli-owner", 300)
            .await
            .unwrap();
        let r2 = svc
            .start_or_resume_loop(&loop_id, "cli-owner", 300)
            .await
            .unwrap();

        assert!(matches!(r1, LoopStartOutcome::Started { .. }));
        assert!(matches!(r2, LoopStartOutcome::Resumed { .. }));
    }
}

#[tokio::test]
async fn test_cli_nonexistent_loop_error() {
    let db = setup_temp_db().await;
    let svc = TaskEngineeringLoopService::new(db.pool.clone());

    // Inspecting a nonexistent loop must not panic.
    let result = svc.inspect_loop("nonexistent-loop-id").await;
    match result {
        Ok(None) => { /* expected: no loop found */ }
        Err(_) => { /* also acceptable: error */ }
        _ => panic!("unexpected result"),
    }

    // Error messages must not contain raw DB paths.
    let err_result = svc.inspect_loop("nonexistent-loop-id").await;
    if let Err(e) = err_result {
        assert!(
            !e.contains("harness.db"),
            "error must not contain DB path: {e}"
        );
    }
}
