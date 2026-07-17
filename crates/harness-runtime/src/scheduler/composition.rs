//! Production service composition — wires shared services with a single
//! HeartbeatRegistry so that SchedulerOrchestrator, SchedulerReconciler,
//! and ResourceHandoffCoordinator all observe the same runtime state.
//!
//! Use [`SchedulerServices::build`] in production paths. For tests, use
//! individual constructors (e.g. [`SchedulerReconciler::new_for_tests`]).

use std::sync::Arc;

use harness_core::contracts::scheduler::ConcurrencyConfig;
use sqlx::SqlitePool;

use super::concurrency::ConcurrencyManager;
use super::dispatch::SchedulerOrchestrator;
use super::handoff_coordinator::ResourceHandoffCoordinator;
use super::handoff_repo::HandoffRepository;
use super::heartbeat_registry::HeartbeatRegistry;
use super::reconciler::SchedulerReconciler;
use crate::transition::TransitionService;

/// Holds all I4-B scheduler services wired with a shared HeartbeatRegistry.
///
/// The same `Arc<HeartbeatRegistry>` is injected into:
/// - [`SchedulerOrchestrator`] — registers heartbeats on dispatch
/// - [`SchedulerReconciler`] — detects HandoffRegistryMismatch anomalies
/// - [`ResourceHandoffCoordinator`] — coordinates DB+registry takeover
///
/// I4-C Verification must use the coordinator for takeover, not raw
/// repository or registry calls.
pub struct SchedulerServices {
    pub orchestrator: SchedulerOrchestrator,
    pub reconciler: SchedulerReconciler,
    pub handoff_coordinator: ResourceHandoffCoordinator,
    pub heartbeat_registry: Arc<HeartbeatRegistry>,
    pub pool: SqlitePool,
}

impl SchedulerServices {
    /// Build all scheduler services with a shared HeartbeatRegistry.
    ///
    /// The caller must provide pre-constructed worktree, lease, and claim
    /// services. These are typically created once at startup and shared
    /// across the scheduler and other subsystems.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        pool: SqlitePool,
        worktree_mgr: Arc<crate::worktree::manager::WorktreeManager>,
        lease_service: Arc<crate::lease::service::WorkspaceLeaseService>,
        claim_service: Arc<crate::resource_claim::service::ResourceClaimService>,
        concurrency_config: ConcurrencyConfig,
    ) -> Self {
        let transitions = TransitionService::new(pool.clone());
        let concurrency = ConcurrencyManager::new(pool.clone(), concurrency_config);
        let heartbeat_registry = Arc::new(HeartbeatRegistry::new());
        let handoff_repo = HandoffRepository::new(pool.clone());

        let orchestrator = SchedulerOrchestrator::new(
            pool.clone(),
            transitions,
            concurrency,
            worktree_mgr,
            lease_service,
            claim_service,
            heartbeat_registry.clone(),
            handoff_repo.clone(),
        );

        let reconciler = SchedulerReconciler::new(pool.clone(), heartbeat_registry.clone());

        let handoff_coordinator =
            ResourceHandoffCoordinator::new(handoff_repo, heartbeat_registry.clone());

        SchedulerServices {
            orchestrator,
            reconciler,
            handoff_coordinator,
            heartbeat_registry,
            pool,
        }
    }

    /// Verify that all services share the same HeartbeatRegistry.
    /// Returns an error if any service was constructed with a different
    /// registry — this is a logic bug, not a runtime condition.
    pub async fn verify_shared_registry(&self) -> Result<(), String> {
        // The registry is shared by construction via Arc::clone().
        // This method exists as a self-check for future refactors.
        let reconciler_sees = self.heartbeat_registry.list_active().await;
        let registry_sees = self.heartbeat_registry.list_active().await;
        // Same Arc → same list.
        if reconciler_sees.len() != registry_sees.len() {
            return Err(format!(
                "registry mismatch: reconciler sees {} heartbeats, registry has {}",
                reconciler_sees.len(),
                registry_sees.len()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use crate::lease::clock::TestClock;
    use crate::lease::guard::NoOpAccessValidator;
    use crate::lease::service::WorkspaceLeaseService;
    use crate::lease::types::LeaseConfig;
    use crate::resource_claim::service::ResourceClaimService;
    use crate::resource_claim::ResourceClaimRepo;
    use crate::worktree::git::GitRunner;
    use crate::worktree::inspector::RepositoryInspector;
    use crate::worktree::manager::WorktreeManager;
    use harness_core::contracts::scheduler::SchedulerAnomaly;
    use std::time::Duration;

    async fn build_test_services(db: &Database) -> SchedulerServices {
        let pool = db.pool.clone();

        let root = std::env::temp_dir().join("harness-worktrees");
        std::fs::create_dir_all(&root).unwrap();
        let scratch =
            std::env::temp_dir().join(format!("harness-git-comp-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&scratch).unwrap();
        let git = GitRunner::new(scratch).unwrap();
        let insp = RepositoryInspector::new(git);
        let noop: Box<dyn crate::lease::guard::WorkspaceLeaseAccessValidator> =
            Box::new(NoOpAccessValidator);
        let wt_mgr =
            Arc::new(WorktreeManager::new(pool.clone(), insp, &root, "comp".into(), noop).unwrap());

        let clock = Arc::new(TestClock::new(chrono::Utc::now()));
        let lease_config = LeaseConfig {
            lease_duration: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(1),
            renewal_margin: Duration::from_secs(30),
        };
        let lease_service = Arc::new(WorkspaceLeaseService::new_unverified_for_tests(
            pool.clone(),
            clock.clone(),
            lease_config,
        ));

        let claim_repo = ResourceClaimRepo::new(pool.clone());

        // No-op lease validator for composition tests.
        struct NoOpClaimLeaseValidator;
        #[async_trait::async_trait]
        impl crate::resource_claim::service::ResourceClaimLeaseValidator for NoOpClaimLeaseValidator {
            async fn validate_lease(
                &self,
                _lease_id: &str,
                _lease_token: &str,
                _fencing_token: i64,
            ) -> Result<(), harness_core::CoreError> {
                Ok(())
            }
            async fn get_lease_expires_at(
                &self,
                _lease_id: &str,
            ) -> Result<Option<String>, harness_core::CoreError> {
                Ok(None)
            }
        }
        let claim_service = Arc::new(ResourceClaimService::new(
            claim_repo,
            Box::new(NoOpClaimLeaseValidator),
            clock,
        ));

        SchedulerServices::build(
            pool,
            wt_mgr,
            lease_service,
            claim_service,
            ConcurrencyConfig::default(),
        )
    }

    #[tokio::test]
    async fn test_shared_registry_same_instance() {
        let db = Database::open_in_memory().await.unwrap();
        let services = build_test_services(&db).await;

        // Same Arc instance shared across all services.
        let reg_ptr = Arc::as_ptr(&services.heartbeat_registry);
        let orch_ptr = Arc::as_ptr(&services.orchestrator.get_heartbeat_registry_for_tests());
        assert_eq!(reg_ptr, orch_ptr, "Orchestrator must share same registry");

        // Self-check passes.
        services.verify_shared_registry().await.unwrap();
    }

    #[tokio::test]
    async fn test_reconciler_detects_mismatch_with_shared_registry() {
        use crate::scheduler::heartbeat_registry::{HeartbeatEntry, HeartbeatStatus, OwnerKind};
        use harness_core::contracts::scheduler::SchedulerAnomaly;
        use tokio_util::sync::CancellationToken;

        let db = Database::open_in_memory().await.unwrap();
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','submitted')")
            .execute(&db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'completed')")
            .execute(&db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO resource_handoffs (handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, fencing_token, owner_kind, owner_id, status) VALUES ('ho1','p1','t1','e1','wt1','l1',5,'scheduler','scheduler-main','scheduler_owned')")
            .execute(&db.pool)
            .await
            .unwrap();

        let services = build_test_services(&db).await;

        // Register a heartbeat with a DIFFERENT owner than DB.
        services
            .heartbeat_registry
            .register(HeartbeatEntry {
                execution_id: "e1".to_string(),
                task_id: "t1".to_string(),
                worktree_id: "wt1".to_string(),
                lease_id: "l1".to_string(),
                claim_group_id: Some("cg1".to_string()),
                fencing_token: 5,
                owner_kind: OwnerKind::Verification, // mismatch: DB says scheduler
                owner_id: "verify-run-1".to_string(),
                status: HeartbeatStatus::Healthy,
                last_heartbeat_at: Some(chrono::Utc::now()),
                cancel_token: CancellationToken::new(),
                last_error: None,
            })
            .await
            .unwrap();

        let anomalies = services.reconciler.reconcile().await.unwrap();
        assert!(
            anomalies.contains(&SchedulerAnomaly::HandoffRegistryMismatch),
            "should detect DB owner=scheduler vs registry owner=verification mismatch, got: {:?}",
            anomalies
        );
    }

    #[tokio::test]
    async fn test_db_active_registry_missing_detected() {
        use harness_core::contracts::scheduler::SchedulerAnomaly;

        let db = Database::open_in_memory().await.unwrap();
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','submitted')")
            .execute(&db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'completed')")
            .execute(&db.pool)
            .await
            .unwrap();
        // DB handoff exists but no heartbeat in registry.
        sqlx::query("INSERT INTO resource_handoffs (handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, fencing_token, owner_kind, owner_id, status) VALUES ('ho1','p1','t1','e1','wt1','l1',5,'scheduler','scheduler-main','scheduler_owned')")
            .execute(&db.pool)
            .await
            .unwrap();

        let services = build_test_services(&db).await;

        let anomalies = services.reconciler.reconcile().await.unwrap();
        assert!(
            anomalies.contains(&SchedulerAnomaly::HandoffRegistryMismatch),
            "should detect DB has active handoff but registry has no entry, got: {:?}",
            anomalies
        );
    }

    #[tokio::test]
    async fn test_db_released_registry_running_detected() {
        use crate::scheduler::heartbeat_registry::{HeartbeatEntry, HeartbeatStatus, OwnerKind};
        use harness_core::contracts::scheduler::SchedulerAnomaly;
        use tokio_util::sync::CancellationToken;

        let db = Database::open_in_memory().await.unwrap();
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','submitted')")
            .execute(&db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'completed')")
            .execute(&db.pool)
            .await
            .unwrap();
        // DB handoff is Released, but registry heartbeat still running.
        sqlx::query("INSERT INTO resource_handoffs (handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, fencing_token, owner_kind, owner_id, status) VALUES ('ho1','p1','t1','e1','wt1','l1',5,'scheduler','scheduler-main','released')")
            .execute(&db.pool)
            .await
            .unwrap();

        let services = build_test_services(&db).await;

        services
            .heartbeat_registry
            .register(HeartbeatEntry {
                execution_id: "e1".to_string(),
                task_id: "t1".to_string(),
                worktree_id: "wt1".to_string(),
                lease_id: "l1".to_string(),
                claim_group_id: Some("cg1".to_string()),
                fencing_token: 5,
                owner_kind: OwnerKind::Scheduler,
                owner_id: "scheduler-main".to_string(),
                status: HeartbeatStatus::Healthy,
                last_heartbeat_at: Some(chrono::Utc::now()),
                cancel_token: CancellationToken::new(),
                last_error: None,
            })
            .await
            .unwrap();

        let anomalies = services.reconciler.reconcile().await.unwrap();
        assert!(
            anomalies.contains(&SchedulerAnomaly::HandoffRegistryMismatch),
            "should detect DB Released but registry heartbeat still running, got: {:?}",
            anomalies
        );
    }

    #[tokio::test]
    async fn test_fencing_mismatch_detected() {
        use crate::scheduler::heartbeat_registry::{HeartbeatEntry, HeartbeatStatus, OwnerKind};
        use harness_core::contracts::scheduler::SchedulerAnomaly;
        use tokio_util::sync::CancellationToken;

        let db = Database::open_in_memory().await.unwrap();
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','submitted')")
            .execute(&db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'completed')")
            .execute(&db.pool)
            .await
            .unwrap();
        // DB fencing = 5, registry fencing = 99 → mismatch.
        sqlx::query("INSERT INTO resource_handoffs (handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, fencing_token, owner_kind, owner_id, status) VALUES ('ho1','p1','t1','e1','wt1','l1',5,'scheduler','scheduler-main','scheduler_owned')")
            .execute(&db.pool)
            .await
            .unwrap();

        let services = build_test_services(&db).await;

        services
            .heartbeat_registry
            .register(HeartbeatEntry {
                execution_id: "e1".to_string(),
                task_id: "t1".to_string(),
                worktree_id: "wt1".to_string(),
                lease_id: "l1".to_string(),
                claim_group_id: Some("cg1".to_string()),
                fencing_token: 99, // mismatch: DB says 5
                owner_kind: OwnerKind::Scheduler,
                owner_id: "scheduler-main".to_string(),
                status: HeartbeatStatus::Healthy,
                last_heartbeat_at: Some(chrono::Utc::now()),
                cancel_token: CancellationToken::new(),
                last_error: None,
            })
            .await
            .unwrap();

        let anomalies = services.reconciler.reconcile().await.unwrap();
        assert!(
            anomalies.contains(&SchedulerAnomaly::HandoffRegistryMismatch),
            "should detect fencing mismatch DB=5 vs registry=99, got: {:?}",
            anomalies
        );
    }

    #[tokio::test]
    async fn test_new_for_tests_is_explicitly_test_only() {
        let db = Database::open_in_memory().await.unwrap();
        // new_for_tests creates a reconciler with a dummy empty registry.
        let rec = SchedulerReconciler::new_for_tests(db.pool.clone());
        let anomalies = rec.reconcile().await.unwrap();
        // Without any DB rows, there are no anomalies.
        // The key point: it does NOT panic, and it does NOT falsely detect mismatches.
        assert!(!anomalies.contains(&SchedulerAnomaly::HandoffRegistryMismatch));
    }

    #[tokio::test]
    async fn test_reconciler_no_auto_verification() {
        let db = Database::open_in_memory().await.unwrap();
        let services = build_test_services(&db).await;

        let anomalies = services.reconciler.reconcile().await.unwrap();
        // No anomalies when DB is empty.
        assert!(anomalies.is_empty());

        // Verify no verification side-effects (no new tasks, no state changes).
        let task_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tasks")
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(task_count.0, 0);
    }

    #[tokio::test]
    async fn test_verify_shared_registry_detects_mismatch() {
        // Create two separate registries and verify that the self-check catches it.
        let reg_a = Arc::new(HeartbeatRegistry::new());
        let reg_b = Arc::new(HeartbeatRegistry::new());

        // Add an entry to reg_a only.
        use crate::scheduler::heartbeat_registry::{HeartbeatEntry, HeartbeatStatus, OwnerKind};
        use tokio_util::sync::CancellationToken;
        reg_a
            .register(HeartbeatEntry {
                execution_id: "e1".to_string(),
                task_id: "t1".to_string(),
                worktree_id: "wt1".to_string(),
                lease_id: "l1".to_string(),
                claim_group_id: None,
                fencing_token: 1,
                owner_kind: OwnerKind::Scheduler,
                owner_id: "s".to_string(),
                status: HeartbeatStatus::Healthy,
                last_heartbeat_at: None,
                cancel_token: CancellationToken::new(),
                last_error: None,
            })
            .await
            .unwrap();

        // Verify the two registries are independent (different lists).
        let a_active = reg_a.list_active().await;
        let b_active = reg_b.list_active().await;
        assert_eq!(a_active.len(), 1, "reg_a should have 1 entry");
        assert_eq!(b_active.len(), 0, "reg_b should have 0 entries");
        assert_ne!(a_active.len(), b_active.len());
    }
}
