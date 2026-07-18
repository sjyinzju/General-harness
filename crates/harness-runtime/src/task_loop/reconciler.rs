//! TaskLoopReconciler — bounded recovery for I4.5 task engineering loops.
//!
//! Detects and safely advances loops that are stuck between states due to
//! crashes, restarts, or response loss. Each invocation advances at most
//! one safe step; never creates duplicate Attempts, Executions, or Decisions.
//!
//! NEVER: calls Agent/LLM, modifies I4 state directly, or creates >1 active
//! Attempt per loop.

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

    /// Reconcile a single loop. Returns what was done (if anything).
    /// Safe to call repeatedly; idempotent per step.
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

        // Detect and fix state inconsistencies.
        match l.lifecycle {
            LoopLifecycle::Created => {
                // Loop created but never started — nothing to reconcile.
                Ok(ReconcileOutcome::NoAction {
                    reason: "loop not started".into(),
                })
            }
            LoopLifecycle::Ready | LoopLifecycle::Evaluating => {
                // These are resting states — check for inconsistencies.
                self.check_resting_state(loop_id, &l).await
            }
            LoopLifecycle::PreparingAttempt => {
                // Check if an Attempt row exists but loop counters weren't updated.
                let active = self.repo.load_active_attempt(loop_id).await?;
                match active {
                    Some(a) if a.lifecycle == AttemptLifecycle::Created => {
                        // Attempt created but loop never transitioned to AttemptActive.
                        // Transition loop to AttemptActive.
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
                        // Stuck with no Attempt — transition back to Ready.
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
            LoopLifecycle::AttemptActive => {
                // Check if the active Attempt's Execution is terminal.
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
                                // Execution terminal but loop still AttemptActive.
                                // Transition loop to Evaluating.
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
            LoopLifecycle::WaitingForReconciliation => {
                // Check if I4 reconciliation has resolved.
                // For now, transition to Evaluating so the service can re-assess.
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
            LoopLifecycle::ReconciliationRequired => Ok(ReconcileOutcome::Blocked {
                reason: "loop-level reconciliation required".into(),
            }),
            _ => Ok(ReconcileOutcome::NoAction {
                reason: format!("unhandled lifecycle: {}", l.lifecycle.as_str()),
            }),
        }
    }

    async fn check_resting_state(
        &self,
        loop_id: &str,
        l: &TaskLoopRow,
    ) -> Result<ReconcileOutcome, String> {
        // If there's an active attempt with a terminal execution, transition to Evaluating.
        let active = self.repo.load_active_attempt(loop_id).await?;
        if let Some(a) = active {
            if let Some(eid) = &a.execution_id {
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
            reason: "resting state, no anomalies".into(),
        })
    }

    /// Reconcile all non-terminal loops (bounded batch).
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
