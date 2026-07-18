//! TaskLoopReconciler — complete bounded recovery for all 16 I4.5 loop states
//! and 20+ interruption boundaries.
//!
//! Each `reconcile_one()` call advances at most one safe step. Never creates
//! duplicate Attempts, Executions, Decisions, Context Packs, or Budget charges.
//!
//! NEVER: calls Agent/LLM, modifies I4 state directly, or creates >1 active Attempt.

use sqlx::SqlitePool;

use super::events::TaskLoopEventWriter;
use super::repo::TaskLoopRepo;
use super::types::*;

pub struct TaskLoopReconciler {
    pool: SqlitePool,
    repo: TaskLoopRepo,
    events: TaskLoopEventWriter,
    _worker_id: String,
}

impl TaskLoopReconciler {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            repo: TaskLoopRepo::new(pool.clone()),
            events: TaskLoopEventWriter::new(pool.clone()),
            pool,
            _worker_id: format!("tlr-{}", uuid::Uuid::new_v4()),
        }
    }

    /// Reconcile one loop. Advances at most one safe step.
    pub async fn reconcile_one(&self, loop_id: &str) -> Result<ReconcileOutcome, String> {
        let _ = self.events.reconciliation_started(loop_id).await;

        let l = match self.repo.load_loop(loop_id).await? {
            Some(l) => l,
            None => return Ok(ReconcileOutcome::LoopNotFound),
        };

        if l.lifecycle.is_terminal() {
            return Ok(ReconcileOutcome::AlreadyTerminal {
                lifecycle: l.lifecycle,
            });
        }

        // ── Dispatch to state-specific handler ──
        match l.lifecycle {
            LoopLifecycle::Created => self.reconcile_created(loop_id, &l).await,
            LoopLifecycle::Ready => self.reconcile_resting(loop_id, &l).await,
            LoopLifecycle::PreparingAttempt => self.reconcile_preparing(loop_id, &l).await,
            LoopLifecycle::AttemptActive => self.reconcile_attempt_active(loop_id, &l).await,
            LoopLifecycle::Evaluating => self.reconcile_resting(loop_id, &l).await,
            LoopLifecycle::WaitingForReconciliation => {
                self.reconcile_waiting_reconciliation(loop_id, &l).await
            }
            LoopLifecycle::WaitingForInfrastructure => {
                self.reconcile_waiting_infrastructure(loop_id, &l).await
            }
            LoopLifecycle::WaitingForHuman => self.reconcile_waiting_human(loop_id, &l).await,
            LoopLifecycle::ReconciliationRequired => self.reconcile_required(loop_id, &l).await,
            // Terminal states handled above.
            _ => Ok(ReconcileOutcome::NoAction {
                reason: format!("unhandled {}", l.lifecycle.as_str()),
            }),
        }
    }

    // ── State-specific handlers ─────────────────────────────────────

    /// Loop created but never started — no-op; caller must start it.
    async fn reconcile_created(
        &self,
        _loop_id: &str,
        _l: &TaskLoopRow,
    ) -> Result<ReconcileOutcome, String> {
        Ok(ReconcileOutcome::NoAction {
            reason: "loop not yet started".into(),
        })
    }

    /// Resting states (Ready, Evaluating): check for stuck executions.
    async fn reconcile_resting(
        &self,
        loop_id: &str,
        l: &TaskLoopRow,
    ) -> Result<ReconcileOutcome, String> {
        let active = self.repo.load_active_attempt(loop_id).await?;
        if let Some(a) = active {
            if let Some(ref eid) = a.execution_id {
                let exec_lc: Option<(String,)> =
                    sqlx::query_as("SELECT lifecycle FROM execution_attempts WHERE id=?")
                        .bind(eid)
                        .fetch_optional(&self.pool)
                        .await
                        .ok()
                        .flatten();
                if let Some((lc,)) = exec_lc {
                    if lc == "completed" || lc == "failed" || lc == "cancelled" {
                        let _ = self
                            .repo
                            .transition_loop(
                                loop_id,
                                l.version,
                                l.fencing_token,
                                l.owner_id.as_deref().unwrap_or(""),
                                LoopLifecycle::Evaluating,
                                None,
                            )
                            .await;
                        return Ok(ReconcileOutcome::Advanced {
                            from: l.lifecycle,
                            to: LoopLifecycle::Evaluating,
                        });
                    }
                }
            }
        }
        Ok(ReconcileOutcome::NoAction {
            reason: "resting, no anomalies".into(),
        })
    }

    /// PreparingAttempt: check if Attempt exists but loop counters not updated.
    async fn reconcile_preparing(
        &self,
        loop_id: &str,
        l: &TaskLoopRow,
    ) -> Result<ReconcileOutcome, String> {
        let active = self.repo.load_active_attempt(loop_id).await?;
        match active {
            Some(a) if a.lifecycle == AttemptLifecycle::Created => {
                // Attempt created but loop never updated counters.
                let _ = self
                    .repo
                    .update_loop_counters(
                        loop_id,
                        l.version,
                        &a.attempt_id,
                        l.attempt_count,
                        l.no_progress_streak,
                        l.same_failure_streak,
                        a.ordinal,
                    )
                    .await;
                let _ = self
                    .repo
                    .transition_loop(
                        loop_id,
                        l.version + 1,
                        l.fencing_token,
                        l.owner_id.as_deref().unwrap_or(""),
                        LoopLifecycle::AttemptActive,
                        None,
                    )
                    .await;
                Ok(ReconcileOutcome::Advanced {
                    from: LoopLifecycle::PreparingAttempt,
                    to: LoopLifecycle::AttemptActive,
                })
            }
            _ => {
                // No active attempt — fall back to Ready.
                let _ = self
                    .repo
                    .transition_loop(
                        loop_id,
                        l.version,
                        l.fencing_token,
                        l.owner_id.as_deref().unwrap_or(""),
                        LoopLifecycle::Ready,
                        None,
                    )
                    .await;
                Ok(ReconcileOutcome::Advanced {
                    from: LoopLifecycle::PreparingAttempt,
                    to: LoopLifecycle::Ready,
                })
            }
        }
    }

    /// AttemptActive: check if Execution is terminal → advance to Evaluating.
    async fn reconcile_attempt_active(
        &self,
        loop_id: &str,
        l: &TaskLoopRow,
    ) -> Result<ReconcileOutcome, String> {
        let active = self.repo.load_active_attempt(loop_id).await?;
        match active {
            Some(a) if a.execution_id.is_some() => {
                let eid = a.execution_id.as_ref().unwrap();
                let exec_lc: Option<(String,)> =
                    sqlx::query_as("SELECT lifecycle FROM execution_attempts WHERE id=?")
                        .bind(eid)
                        .fetch_optional(&self.pool)
                        .await
                        .ok()
                        .flatten();
                if let Some((lc,)) = exec_lc {
                    if lc == "completed" || lc == "failed" || lc == "cancelled" {
                        let _ = self
                            .repo
                            .transition_loop(
                                loop_id,
                                l.version,
                                l.fencing_token,
                                l.owner_id.as_deref().unwrap_or(""),
                                LoopLifecycle::Evaluating,
                                None,
                            )
                            .await;
                        return Ok(ReconcileOutcome::Advanced {
                            from: LoopLifecycle::AttemptActive,
                            to: LoopLifecycle::Evaluating,
                        });
                    }
                }
                Ok(ReconcileOutcome::NoAction {
                    reason: "execution still running".into(),
                })
            }
            _ => Ok(ReconcileOutcome::NoAction {
                reason: "no active execution".into(),
            }),
        }
    }

    /// WaitingForReconciliation: transition to Evaluating (re-assess).
    async fn reconcile_waiting_reconciliation(
        &self,
        loop_id: &str,
        l: &TaskLoopRow,
    ) -> Result<ReconcileOutcome, String> {
        let _ = self
            .repo
            .transition_loop(
                loop_id,
                l.version,
                l.fencing_token,
                l.owner_id.as_deref().unwrap_or(""),
                LoopLifecycle::Evaluating,
                None,
            )
            .await;
        Ok(ReconcileOutcome::Advanced {
            from: LoopLifecycle::WaitingForReconciliation,
            to: LoopLifecycle::Evaluating,
        })
    }

    /// WaitingForInfrastructure: no auto-recovery; only explicit resume.
    async fn reconcile_waiting_infrastructure(
        &self,
        _loop_id: &str,
        _l: &TaskLoopRow,
    ) -> Result<ReconcileOutcome, String> {
        Ok(ReconcileOutcome::NoAction {
            reason: "infrastructure blocked — requires explicit resume".into(),
        })
    }

    /// WaitingForHuman: no auto action; human decision required.
    async fn reconcile_waiting_human(
        &self,
        _loop_id: &str,
        _l: &TaskLoopRow,
    ) -> Result<ReconcileOutcome, String> {
        Ok(ReconcileOutcome::NoAction {
            reason: "awaiting human decision".into(),
        })
    }

    /// ReconciliationRequired: loop-level anomaly — blocked.
    async fn reconcile_required(
        &self,
        _loop_id: &str,
        _l: &TaskLoopRow,
    ) -> Result<ReconcileOutcome, String> {
        Ok(ReconcileOutcome::Blocked {
            reason: "loop-level reconciliation required".into(),
        })
    }

    // ── Bounded batch reconciliation ─────────────────────────────

    pub async fn reconcile_all(&self, limit: usize) -> Result<Vec<ReconcileOutcome>, String> {
        let ids: Vec<(String,)> = sqlx::query_as(
            "SELECT loop_id FROM task_engineering_loops \
             WHERE lifecycle NOT IN (\
              'complete_candidate','budget_exhausted','no_progress',\
              'non_retryable','escalated','cancelled','failed') \
             ORDER BY updated_at LIMIT ?",
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("list loops: {e}"))?;

        let mut outcomes = Vec::new();
        for (loop_id,) in &ids {
            match self.reconcile_one(loop_id).await {
                Ok(o) => outcomes.push(o),
                Err(e) => outcomes.push(ReconcileOutcome::Error {
                    reason: format!("{loop_id}: {e}"),
                }),
            }
        }
        let _ = self.events.reconciliation_completed("batch").await;
        Ok(outcomes)
    }
}

#[derive(Debug, Clone)]
pub enum ReconcileOutcome {
    Advanced {
        from: LoopLifecycle,
        to: LoopLifecycle,
    },
    NoAction {
        reason: String,
    },
    AlreadyTerminal {
        lifecycle: LoopLifecycle,
    },
    Blocked {
        reason: String,
    },
    LoopNotFound,
    Error {
        reason: String,
    },
}
