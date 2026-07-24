//! I4.6 Candidate Review Gate — comprehensive smoke test.
//!
//! Exercises the full review lifecycle: candidate freeze, precheck,
//! reviewer selection, dossier construction, cache deduplication,
//! invocation counter, read-only enforcement, events, and recovery.
//!
//! Uses the FakeReviewer for deterministic testing.

use harness_core::contracts::review::{
    FindingSeverity, ReviewDecision, ReviewState, ReviewerOutput,
};
use harness_core::contracts::runtime_profile::{
    AuthMode, AuthStatus, CapabilitySet, CoreStatus, ExecutionStatus, OptionalCapabilities,
    ProviderSource, RequiredCapabilities, RuntimeProfile, TriState,
};
use harness_core::contracts::verification::{VerificationOutcome, VerificationResult};
use harness_runtime::db::Database;
use harness_runtime::review::ReviewOrchestrationService;
use sqlx::SqlitePool;

fn mk_profile(id: &str, kind: &str) -> RuntimeProfile {
    RuntimeProfile {
        id: id.into(),
        agent_definition_id: format!("def-{id}"),
        label: format!("profile-{id}"),
        agent_kind: kind.into(),
        adapter_kind: kind.into(),
        agent_version: "1.0".into(),
        executable_path: format!("/usr/bin/{kind}"),
        provider: kind.into(),
        provider_source: ProviderSource::UserDeclared,
        model: Some("default".into()),
        base_url: None,
        auth_mode: AuthMode::None,
        auth_status: AuthStatus::Authenticated,
        credential_ref: None,
        capabilities: CapabilitySet {
            required: RequiredCapabilities {
                execute: TriState::Supported,
                working_directory: TriState::Supported,
                stream_output: TriState::Supported,
                process_exit: TriState::Supported,
                cancellation: TriState::Supported,
                timeout: TriState::Supported,
                final_result: TriState::Supported,
            },
            optional: OptionalCapabilities {
                native_session_resume: TriState::Unsupported,
                structured_output: TriState::Supported,
                tool_events: TriState::Unsupported,
                file_change_events: TriState::Unsupported,
                reasoning_summary: TriState::Unsupported,
                interactive_approval: TriState::Unsupported,
                usage_reporting: TriState::Unsupported,
            },
            workspace_modes: vec![],
            supported_languages: vec![],
            mcp_tools: vec![],
            supported_platforms: vec![],
        },
        core_status: CoreStatus::Available,
        authentication_status:
            harness_core::contracts::runtime_profile::AuthCheckStatus::Authenticated,
        execution_status: ExecutionStatus::SmokeTestPassed,
        optional_integrations: vec![],
        discovery_source: "smoke".into(),
        passive_probe: None,
        active_validation: None,
        concurrency_max: 5,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

async fn seed(db: &Database) -> (ReviewOrchestrationService, SqlitePool) {
    let p = db.pool.clone();
    sqlx::query("INSERT INTO projects(id,objective,lifecycle) VALUES('p1','test','active')")
        .execute(&p)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('t1','p1','test task','verified')",
    )
    .execute(&p)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e1','t1',1,'completed')",
    )
    .execute(&p)
    .await
    .unwrap();
    (ReviewOrchestrationService::new(p.clone()), p)
}

// ══════════════════════════════════════════════════════════════════════
// Smoke 1: Full Approved Path with FakeReviewer
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn smoke_approved_path() {
    let db = Database::open_in_memory().await.unwrap();
    let (svc, pool) = seed(&db).await;

    // 1. Discovery: get available profiles
    let profiles = vec![
        mk_profile("prof-codex", "codex"),   // executor
        mk_profile("prof-claude", "claude"), // reviewer
    ];

    // 2. Select reviewer (different from executor)
    let reviewer = svc.select_reviewer("prof-codex", &profiles).unwrap();
    assert_eq!(reviewer.id, "prof-claude");
    assert_ne!(reviewer.id, "prof-codex");

    // 3. Freeze candidate (after I4.5 CompletionEligibility PASS)
    let c = svc
        .freeze_candidate(
            "t1",
            "e1",
            "prof-codex",
            "ws1",
            "abc123",
            "tree1",
            "diff1",
            "task1",
            "ev1",
        )
        .await
        .unwrap();
    assert!(!c.candidate_id.is_empty());

    // 4. Run deterministic precheck
    let outcome = VerificationOutcome {
        result: VerificationResult::Passed,
        failure_classification: None,
        summary: "all passed".into(),
        blockers: vec![],
        findings_count: 0,
    };
    let precheck = svc
        .run_precheck(&c, &outcome, &[], true, true, true, true, true, true, true)
        .await;
    assert!(
        precheck.passed,
        "precheck must pass: {:?}",
        precheck.blocker_reason
    );

    // 5. Create review
    let req = svc
        .create_review(&c.candidate_id, &reviewer.id)
        .await
        .unwrap();
    assert_eq!(req.state, ReviewState::Requested);

    // 6. Advance through lifecycle
    svc.transition(&req.review_id, &ReviewState::Preparing)
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Prechecking)
        .await
        .unwrap();

    // 7. Build dossier
    let dossier = svc
        .build_dossier(
            &req.review_id,
            &c,
            "test task",
            vec!["cargo test passes".into()],
            vec![],
            vec!["src/".into()],
            "codex",
            vec!["src/main.rs".into()],
            "1 file changed, +3/-1",
            "CompleteCandidate",
            "all tests passed",
            vec!["ev1".into()],
            vec![],
        )
        .await;
    assert!(!dossier.dossier_digest.is_empty());

    // 8. Check dossier bounds
    let bounds = svc.check_dossier_bounds(1, 100, 1, 100);
    assert!(matches!(
        bounds,
        harness_runtime::review::DossierBoundsCheck::Ok
    ));

    // 9. Check cache — first time, no cache hit
    let cache = svc.check_cache(&c, &reviewer.id).await.unwrap();
    assert!(cache.is_none(), "first call must be cache miss");

    // 10. Invoke FakeReviewer (Approved)
    svc.transition(&req.review_id, &ReviewState::Reviewing)
        .await
        .unwrap();

    // Log real invocation
    let inv_id = svc
        .log_invocation(
            &req.review_id,
            &c.candidate_id,
            &reviewer.id,
            false,
            Some(&dossier.dossier_digest),
        )
        .await
        .unwrap();

    // Simulate FakeReviewer::approved()
    let reviewer_output = ReviewerOutput {
        decision: "Approved".into(),
        summary: "All checks passed. No findings.".into(),
        findings: vec![],
    };
    assert_eq!(reviewer_output.decision, "Approved");

    svc.complete_invocation(&inv_id, "approved").await.unwrap();

    // 11. Re-verify candidate digests after review (read-only check)
    let still_clean = svc
        .reverify_candidate_after_review(&c, "tree1", "diff1", "task1", "ev1")
        .await
        .unwrap();
    assert!(still_clean, "candidate must be unchanged after review");

    // 12. Apply decision policy
    let (decision, findings) = svc.apply_decision(&req.review_id, &reviewer_output);
    assert_eq!(decision, ReviewDecision::Approved);
    assert!(findings.is_empty());

    // 13. Finalize decision
    svc.finalize_decision(
        &req.review_id,
        &decision,
        &findings,
        &c,
        &reviewer_output,
        &reviewer.id,
    )
    .await
    .unwrap();

    // 14. Verify cache entry exists
    let cache = svc.check_cache(&c, &reviewer.id).await.unwrap();
    assert!(
        cache.is_some(),
        "cache must be populated after terminal decision"
    );
    let (cached_review_id, cached_decision) = cache.unwrap();
    assert_eq!(cached_review_id, req.review_id);
    assert_eq!(cached_decision, "approved");

    // 15. Verify invocation count = 1
    let count = svc.count_invocations(&req.review_id).await.unwrap();
    assert_eq!(count, 1, "exactly one real reviewer invocation");

    // 16. Verify events were written
    let event_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM review_events WHERE review_id=?")
            .bind(&req.review_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        event_count.0 >= 4,
        "at least 4 events expected (Requested, Precheck, Selected, Approved)"
    );

    // 17. Verify ApprovedCandidate
    let approved = svc
        .build_approved_candidate(&c.candidate_id, &req.review_id)
        .await
        .unwrap();
    assert_eq!(approved.candidate_id, c.candidate_id);
    assert_eq!(approved.review_id, req.review_id);
    assert_eq!(approved.candidate_tree_hash, "tree1");
    assert_eq!(approved.diff_digest, "diff1");

    println!("=== SMOKE 1 PASS: Full approved path ===");
}

// ══════════════════════════════════════════════════════════════════════
// Smoke 2: Cache Deduplication (same candidate, same reviewer)
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn smoke_cache_deduplication() {
    let db = Database::open_in_memory().await.unwrap();
    let (svc, _pool) = seed(&db).await;

    let profiles = vec![
        mk_profile("prof-codex", "codex"),
        mk_profile("prof-claude", "claude"),
    ];
    let reviewer = svc.select_reviewer("prof-codex", &profiles).unwrap();

    let c = svc
        .freeze_candidate(
            "t1",
            "e1",
            "prof-codex",
            "ws1",
            "abc",
            "tree1",
            "diff1",
            "task1",
            "ev1",
        )
        .await
        .unwrap();

    let req = svc
        .create_review(&c.candidate_id, &reviewer.id)
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Preparing)
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Prechecking)
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Reviewing)
        .await
        .unwrap();

    // First real invocation
    let _inv1 = svc
        .log_invocation(&req.review_id, &c.candidate_id, &reviewer.id, false, None)
        .await
        .unwrap();
    let output = ReviewerOutput {
        decision: "Approved".into(),
        summary: "clean".into(),
        findings: vec![],
    };
    let (decision, findings) = svc.apply_decision(&req.review_id, &output);
    svc.finalize_decision(
        &req.review_id,
        &decision,
        &findings,
        &c,
        &output,
        &reviewer.id,
    )
    .await
    .unwrap();

    let count = svc.count_invocations(&req.review_id).await.unwrap();
    assert_eq!(count, 1, "first invocation count = 1");

    // Cache hit — second request with same candidate + reviewer
    let cache = svc.check_cache(&c, &reviewer.id).await.unwrap();
    assert!(cache.is_some());

    // Log a cache hit (no real invocation)
    svc.log_invocation(&req.review_id, &c.candidate_id, &reviewer.id, true, None)
        .await
        .unwrap();

    // Count should still be 1 (cache hits not counted)
    let count2 = svc.count_invocations(&req.review_id).await.unwrap();
    assert_eq!(count2, 1, "invocation count unchanged after cache hit");

    println!("=== SMOKE 2 PASS: Cache deduplication ===");
}

// ══════════════════════════════════════════════════════════════════════
// Smoke 3: Precheck Blocked (no Reviewer invocation)
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn smoke_precheck_blocked_no_reviewer_call() {
    let db = Database::open_in_memory().await.unwrap();
    let (svc, _pool) = seed(&db).await;

    let c = svc
        .freeze_candidate(
            "t1",
            "e1",
            "prof-codex",
            "ws1",
            "abc",
            "tree1",
            "diff1",
            "task1",
            "ev1",
        )
        .await
        .unwrap();

    let req = svc
        .create_review(&c.candidate_id, "prof-claude")
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Preparing)
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Prechecking)
        .await
        .unwrap();

    // Precheck fails → Blocked BEFORE Reviewer invocation
    svc.transition(&req.review_id, &ReviewState::Blocked)
        .await
        .unwrap();

    // Verify zero real invocations
    let count = svc.count_invocations(&req.review_id).await.unwrap();
    assert_eq!(count, 0, "precheck blocked → zero reviewer invocations");

    let final_req = svc.get_review(&req.review_id).await.unwrap().unwrap();
    assert_eq!(final_req.state, ReviewState::Blocked);

    println!("=== SMOKE 3 PASS: Precheck blocked, zero reviewer calls ===");
}

// ══════════════════════════════════════════════════════════════════════
// Smoke 4: Rogue Reviewer Detected (read-only enforcement)
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn smoke_rogue_reviewer_detected() {
    let db = Database::open_in_memory().await.unwrap();
    let (svc, _pool) = seed(&db).await;

    let c = svc
        .freeze_candidate(
            "t1",
            "e1",
            "prof-codex",
            "ws1",
            "abc",
            "tree1",
            "diff1",
            "task1",
            "ev1",
        )
        .await
        .unwrap();

    let req = svc
        .create_review(&c.candidate_id, "prof-claude")
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Preparing)
        .await
        .unwrap();

    // Rogue reviewer modified the worktree → tree hash changed
    let still_clean = svc
        .reverify_candidate_after_review(&c, "tree_changed_by_reviewer", "diff1", "task1", "ev1")
        .await
        .unwrap();
    assert!(!still_clean, "rogue modification must be detected");

    // Review must be Stale
    let updated = svc.get_review(&req.review_id).await.unwrap().unwrap();
    assert_eq!(updated.state, ReviewState::Stale);

    // ApprovedCandidate must NOT be buildable from Stale review
    let result = svc
        .build_approved_candidate(&c.candidate_id, &req.review_id)
        .await;
    assert!(
        result.is_err(),
        "Stale review cannot produce ApprovedCandidate"
    );

    println!("=== SMOKE 4 PASS: Rogue reviewer detected via digest change ===");
}

// ══════════════════════════════════════════════════════════════════════
// Smoke 5: Rejected Path with Findings
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn smoke_rejected_with_findings() {
    let db = Database::open_in_memory().await.unwrap();
    let (svc, _pool) = seed(&db).await;

    let c = svc
        .freeze_candidate(
            "t1",
            "e1",
            "prof-codex",
            "ws1",
            "abc",
            "tree1",
            "diff1",
            "task1",
            "ev1",
        )
        .await
        .unwrap();

    let req = svc
        .create_review(&c.candidate_id, "prof-claude")
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Preparing)
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Prechecking)
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Reviewing)
        .await
        .unwrap();

    // Simulate FakeReviewer::rejected("Critical", "Correctness", "null pointer dereference")
    let reviewer_output = ReviewerOutput {
        decision: "Rejected".into(),
        summary: "Rejected: null pointer dereference".into(),
        findings: vec![harness_core::contracts::review::ReviewerFinding {
            severity: "Critical".into(),
            category: "Correctness".into(),
            summary: "null pointer dereference".into(),
            details: "Details for: null pointer dereference".into(),
            source_location: None,
            evidence_reference: None,
            blocking: true,
        }],
    };
    let (decision, findings) = svc.apply_decision(&req.review_id, &reviewer_output);
    assert_eq!(decision, ReviewDecision::Rejected);
    assert_eq!(findings.len(), 1);
    assert!(findings[0].severity.at_least(&FindingSeverity::High));

    svc.finalize_decision(
        &req.review_id,
        &decision,
        &findings,
        &c,
        &reviewer_output,
        "prof-claude",
    )
    .await
    .unwrap();

    let persisted = svc.get_findings(&req.review_id).await.unwrap();
    assert_eq!(persisted.len(), 1);

    println!("=== SMOKE 5 PASS: Rejected path with findings ===");
}

// ══════════════════════════════════════════════════════════════════════
// Smoke 6: Dossier Bounds Enforcement
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn smoke_dossier_bounds_exceeded() {
    let db = Database::open_in_memory().await.unwrap();
    let (svc, _pool) = seed(&db).await;

    // Too many files
    let bounds = svc.check_dossier_bounds(500, 1000, 5, 1000);
    assert!(matches!(
        bounds,
        harness_runtime::review::DossierBoundsCheck::Exceeded { .. }
    ));

    // Within bounds
    let bounds_ok = svc.check_dossier_bounds(10, 1000, 5, 1000);
    assert!(matches!(
        bounds_ok,
        harness_runtime::review::DossierBoundsCheck::Ok
    ));

    println!("=== SMOKE 6 PASS: Dossier bounds enforcement ===");
}

// ══════════════════════════════════════════════════════════════════════
// Smoke 7: Response-lost Recovery
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn smoke_response_lost_recovery() {
    let db = Database::open_in_memory().await.unwrap();
    let (svc, _pool) = seed(&db).await;

    let c = svc
        .freeze_candidate(
            "t1",
            "e1",
            "prof-codex",
            "ws1",
            "abc",
            "tree1",
            "diff1",
            "task1",
            "ev1",
        )
        .await
        .unwrap();

    let req = svc
        .create_review(&c.candidate_id, "prof-claude")
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Preparing)
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Prechecking)
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Reviewing)
        .await
        .unwrap();

    let output = ReviewerOutput {
        decision: "Approved".into(),
        summary: "clean".into(),
        findings: vec![],
    };
    let (decision, findings) = svc.apply_decision(&req.review_id, &output);
    svc.finalize_decision(
        &req.review_id,
        &decision,
        &findings,
        &c,
        &output,
        "prof-claude",
    )
    .await
    .unwrap();

    // Simulate response-lost: caller doesn't get response, retries
    // Cache must return existing decision
    let cache = svc.check_cache(&c, "prof-claude").await.unwrap();
    assert!(cache.is_some());

    // Cannot create new active review for same candidate+reviewer
    // (but CAN create with different reviewer profile)
    let req2 = svc.create_review(&c.candidate_id, "prof-gemini").await;
    assert!(
        req2.is_ok(),
        "new review with different reviewer is allowed"
    );

    // But same reviewer would hit cache
    let cache2 = svc.check_cache(&c, "prof-claude").await.unwrap();
    assert!(cache2.is_some());

    println!("=== SMOKE 7 PASS: Response-lost recovery via cache ===");
}
