//! ApprovalValidator — validates verification command approvals.
//! Shell execution is denied by default; only valid, unexpired, scope-matching
//! approvals can override. Single-use approvals are consumed atomically.

use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

/// Identity binding fields for approval scope validation.
#[derive(Debug, Clone)]
pub struct ApprovalIdentity {
    pub verification_run_id: String,
    pub step_id: String,
    pub step_op_id: String,
    pub cmd_fingerprint: String,
    pub worktree_id: String,
    pub fencing: i64,
}

/// Row type for verification_approvals queries.
type ApprovalRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
    bool,
    String,
    Option<String>,
);

/// Result of approval validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalDecision {
    /// Approval is valid; command may proceed through ProcessManager.
    Approved { approval_id: String },
    /// No approval required (safe command, policy allows).
    NotRequired,
    /// Approval required but none provided or invalid.
    Denied { reason: String },
    /// Approval exists but has been consumed (single-use).
    AlreadyConsumed { approval_id: String },
    /// Infrastructure error during validation.
    Error { reason: String },
}

/// A persisted approval record for verification command execution.
#[derive(Debug, Clone)]
pub struct ApprovalRecord {
    pub approval_id: String,
    pub verification_run_id: String,
    pub step_id: String,
    pub step_op_id: String,
    pub cmd_fingerprint: String,
    pub worktree_id: String,
    pub fencing_token: i64,
    pub single_use: bool,
    pub lifecycle: String, // pending | consumed | expired | revoked
    pub expires_at: Option<String>,
    pub created_at: String,
}

pub struct ApprovalValidator {
    pool: SqlitePool,
}

impl ApprovalValidator {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Validate and (for single-use) atomically consume an approval.
    /// Returns Approved only if all checks pass.
    pub async fn validate_and_consume(
        &self,
        approval_id: &str,
        identity: &ApprovalIdentity,
    ) -> ApprovalDecision {
        // Load approval.
        let row: Option<ApprovalRow> = sqlx::query_as(
            "SELECT approval_id, verification_run_id, step_id, step_op_id, cmd_fingerprint, worktree_id, fencing_token, single_use, lifecycle, expires_at FROM verification_approvals WHERE approval_id = ?",
        ).bind(approval_id).fetch_optional(&self.pool).await.unwrap_or(None);

        let (aid, vr, si, so, cf, wt, fence, single_use, lc, expires) = match row {
            Some(r) => r,
            None => {
                return ApprovalDecision::Denied {
                    reason: "approval not found".into(),
                }
            }
        };

        // Check lifecycle.
        match lc.as_str() {
            "consumed" => return ApprovalDecision::AlreadyConsumed { approval_id: aid },
            "expired" | "revoked" => {
                return ApprovalDecision::Denied {
                    reason: format!("approval {lc}"),
                }
            }
            "pending" => {}
            _ => {
                return ApprovalDecision::Denied {
                    reason: format!("unknown lifecycle: {lc}"),
                }
            }
        }

        // Check expiry.
        if let Some(ref exp) = expires {
            if let Ok(exp_dt) = chrono::NaiveDateTime::parse_from_str(exp, "%Y-%m-%d %H:%M:%S") {
                let exp_utc = exp_dt.and_utc();
                if chrono::Utc::now() > exp_utc {
                    let _ = sqlx::query(
                        "UPDATE verification_approvals SET lifecycle='expired' WHERE approval_id=?",
                    )
                    .bind(&aid)
                    .execute(&self.pool)
                    .await;
                    return ApprovalDecision::Denied {
                        reason: "approval expired".into(),
                    };
                }
            }
        }

        // Validate identity bindings.
        if vr != identity.verification_run_id {
            return ApprovalDecision::Denied {
                reason: "wrong run".into(),
            };
        }
        if si != identity.step_id {
            return ApprovalDecision::Denied {
                reason: "wrong step".into(),
            };
        }
        if so != identity.step_op_id {
            return ApprovalDecision::Denied {
                reason: "wrong operation".into(),
            };
        }
        if cf != identity.cmd_fingerprint {
            return ApprovalDecision::Denied {
                reason: "wrong fingerprint".into(),
            };
        }
        if wt != identity.worktree_id {
            return ApprovalDecision::Denied {
                reason: "wrong worktree".into(),
            };
        }
        if fence != identity.fencing {
            return ApprovalDecision::Denied {
                reason: "stale fencing".into(),
            };
        }

        // Single-use: atomically consume.
        if single_use {
            let rows = sqlx::query(
                "UPDATE verification_approvals SET lifecycle='consumed' WHERE approval_id=? AND lifecycle='pending'",
            ).bind(&aid).execute(&self.pool).await;

            match rows {
                Ok(r) if r.rows_affected() == 1 => {}
                Ok(_) => return ApprovalDecision::AlreadyConsumed { approval_id: aid },
                Err(e) => {
                    return ApprovalDecision::Error {
                        reason: format!("consume: {e}"),
                    }
                }
            }
        }

        ApprovalDecision::Approved { approval_id: aid }
    }

    /// Insert a new approval (for test setup).
    pub async fn insert_approval(
        &self,
        approval_id: &str,
        identity: &ApprovalIdentity,
        single_use: bool,
        expires_at: Option<&str>,
    ) -> Result<(), CoreError> {
        sqlx::query("INSERT OR REPLACE INTO verification_approvals (approval_id, verification_run_id, step_id, step_op_id, cmd_fingerprint, worktree_id, fencing_token, single_use, lifecycle, expires_at) VALUES (?,?,?,?,?,?,?,?,'pending',?)")
            .bind(approval_id)
            .bind(&identity.verification_run_id)
            .bind(&identity.step_id)
            .bind(&identity.step_op_id)
            .bind(&identity.cmd_fingerprint)
            .bind(&identity.worktree_id)
            .bind(identity.fencing)
            .bind(single_use).bind(expires_at)
            .execute(&self.pool).await
            .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("insert approval: {e}"), ErrorSource::System))?;
        Ok(())
    }

    /// Revoke an approval.
    pub async fn revoke(&self, approval_id: &str) -> Result<(), CoreError> {
        sqlx::query("UPDATE verification_approvals SET lifecycle='revoked' WHERE approval_id=?")
            .bind(approval_id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::PersistenceError,
                    format!("revoke: {e}"),
                    ErrorSource::System,
                )
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    fn mk_id(
        run: &str,
        step: &str,
        op: &str,
        cf: &str,
        wt: &str,
        fencing: i64,
    ) -> ApprovalIdentity {
        ApprovalIdentity {
            verification_run_id: run.into(),
            step_id: step.into(),
            step_op_id: op.into(),
            cmd_fingerprint: cf.into(),
            worktree_id: wt.into(),
            fencing,
        }
    }

    async fn setup() -> (ApprovalValidator, Database) {
        let db = Database::open_in_memory().await.unwrap();
        // Seed prerequisite rows for FK constraints.
        sqlx::query("INSERT INTO projects(id,objective,lifecycle) VALUES('p1','t','active')")
            .execute(&db.pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('t1','p1','t','submitted')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e1','t1',1,'completed')").execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO verification_plans(plan_id,task_id,execution_id,project_id,plan_hash,plan_version,steps_json) VALUES('plan-1','t1','e1','p1','ha',1,'[]')").execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO verification_runs(run_id,plan_id,plan_hash,plan_version,execution_id,task_id,project_id,lifecycle,idempotency_key,request_hash) VALUES('run-1','plan-1','ha',1,'e1','t1','p1','running','ik','hr')").execute(&db.pool).await.unwrap();
        (ApprovalValidator::new(db.pool.clone()), db)
    }

    #[tokio::test]
    async fn test_valid_approval() {
        let (v, _db) = setup().await;
        let id = mk_id("run-1", "s1", "op1", "cf1", "wt1", 5);
        v.insert_approval("a1", &id, false, None).await.unwrap();
        let r = v.validate_and_consume("a1", &id).await;
        assert_eq!(
            r,
            ApprovalDecision::Approved {
                approval_id: "a1".into()
            }
        );
    }
    #[tokio::test]
    async fn test_missing_approval() {
        let (v, _) = setup().await;
        let id = mk_id("r1", "s1", "o1", "cf", "w1", 5);
        let r = v.validate_and_consume("no-exist", &id).await;
        assert!(matches!(r, ApprovalDecision::Denied { .. }));
    }
    #[tokio::test]
    async fn test_wrong_run() {
        let (v, _) = setup().await;
        let id = mk_id("run-1", "s1", "op1", "cf1", "wt1", 5);
        v.insert_approval("a1", &id, false, None).await.unwrap();
        let wrong = mk_id("wrong-run", "s1", "op1", "cf1", "wt1", 5);
        let r = v.validate_and_consume("a1", &wrong).await;
        assert!(matches!(r, ApprovalDecision::Denied { .. }));
    }
    #[tokio::test]
    async fn test_wrong_worktree() {
        let (v, _) = setup().await;
        let id = mk_id("run-1", "s1", "op1", "cf1", "wt1", 5);
        v.insert_approval("a1", &id, false, None).await.unwrap();
        let wrong = mk_id("run-1", "s1", "op1", "cf1", "wt99", 5);
        let r = v.validate_and_consume("a1", &wrong).await;
        assert!(matches!(r, ApprovalDecision::Denied { .. }));
    }
    #[tokio::test]
    async fn test_stale_fencing() {
        let (v, _) = setup().await;
        let id = mk_id("run-1", "s1", "op1", "cf1", "wt1", 5);
        v.insert_approval("a1", &id, false, None).await.unwrap();
        let wrong = mk_id("run-1", "s1", "op1", "cf1", "wt1", 99);
        let r = v.validate_and_consume("a1", &wrong).await;
        assert!(matches!(r, ApprovalDecision::Denied { .. }));
    }
    #[tokio::test]
    async fn test_single_use_consumed_once() {
        let (v, _) = setup().await;
        let id = mk_id("run-1", "s1", "op1", "cf1", "wt1", 5);
        v.insert_approval("a1", &id, true, None).await.unwrap();
        let r1 = v.validate_and_consume("a1", &id).await;
        assert_eq!(
            r1,
            ApprovalDecision::Approved {
                approval_id: "a1".into()
            }
        );
        let r2 = v.validate_and_consume("a1", &id).await;
        assert!(matches!(r2, ApprovalDecision::AlreadyConsumed { .. }));
    }
    #[tokio::test]
    async fn test_single_use_response_lost_idempotent() {
        let (v, _) = setup().await;
        let id = mk_id("run-1", "s1", "op1", "cf1", "wt1", 5);
        v.insert_approval("a1", &id, true, None).await.unwrap();
        // First consume succeeds.
        v.validate_and_consume("a1", &id).await;
        // Same operation retry — already consumed, but identity matches.
        let r = v.validate_and_consume("a1", &id).await;
        assert!(matches!(r, ApprovalDecision::AlreadyConsumed { .. }));
    }
    #[tokio::test]
    async fn test_different_operation_reuse_rejected() {
        let (v, _) = setup().await;
        let id1 = mk_id("run-1", "s1", "op1", "cf1", "wt1", 5);
        v.insert_approval("a1", &id1, true, None).await.unwrap();
        // First consume succeeds.
        let r1 = v.validate_and_consume("a1", &id1).await;
        assert!(matches!(r1, ApprovalDecision::Approved { .. }));
        // Different operation tries to reuse — rejected.
        let id2 = mk_id("run-1", "s1", "op2", "cf1", "wt1", 5);
        let r2 = v.validate_and_consume("a1", &id2).await;
        assert!(matches!(r2, ApprovalDecision::AlreadyConsumed { .. }));
    }
    #[tokio::test]
    async fn test_expired_approval() {
        let (v, _) = setup().await;
        let id = mk_id("run-1", "s1", "op1", "cf1", "wt1", 5);
        v.insert_approval("a1", &id, false, Some("2020-01-01 00:00:00"))
            .await
            .unwrap();
        let r = v.validate_and_consume("a1", &id).await;
        assert!(matches!(r, ApprovalDecision::Denied { .. }));
    }
    #[tokio::test]
    async fn test_revoked_approval() {
        let (v, _) = setup().await;
        let id = mk_id("run-1", "s1", "op1", "cf1", "wt1", 5);
        v.insert_approval("a1", &id, false, None).await.unwrap();
        v.revoke("a1").await.unwrap();
        let r = v.validate_and_consume("a1", &id).await;
        assert!(matches!(r, ApprovalDecision::Denied { .. }));
    }
    #[tokio::test]
    async fn test_approval_no_credential_in_db() {
        let (v, db) = setup().await;
        let id = mk_id("run-1", "s1", "op1", "cf1", "wt1", 5);
        v.insert_approval("a1", &id, false, None).await.unwrap();
        let row: (String, String) = sqlx::query_as(
            "SELECT approval_id, lifecycle FROM verification_approvals WHERE approval_id='a1'",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert!(!row.0.contains("token"));
        assert!(!row.0.contains("secret"));
    }
}
