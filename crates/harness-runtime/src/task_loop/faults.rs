//! I4.5 Fault injection — production-interface wrapper for integration testing.
//!
//! Faults intercept the actual repository/gateway calls, never fake errors
//! by deleting database rows. The production constructor installs an empty
//! plan; faults are consumed (one-shot) so a resumed run proceeds normally.
//!
//! NEVER: persists secrets, modifies I4 state, or affects default production.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Kinds of injectable faults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultKind {
    FailBeforeEffect,
    FailAfterEffectBeforeResponse,
    ResponseLostAfterSuccess,
    CrashAfterDurableWrite,
    FailNthCall,
    OwnerTakeover,
    ObservedStateMutation,
}

/// Where a fault can be injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultBoundary {
    LoopInsert,
    LoopOwnership,
    AttemptInsert,
    BudgetReservation,
    ProfileSelection,
    ExecutionCreate,
    ExecutionBind,
    Dispatch,
    OutcomeObserve,
    DossierRead,
    DecisionInsert,
    ContextPackInsert,
    UsageWrite,
    WorkspaceContinuation,
    EventWrite,
    TerminalTransition,
}

impl FaultBoundary {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LoopInsert => "loop_insert",
            Self::LoopOwnership => "loop_ownership",
            Self::AttemptInsert => "attempt_insert",
            Self::BudgetReservation => "budget_reservation",
            Self::ProfileSelection => "profile_selection",
            Self::ExecutionCreate => "execution_create",
            Self::ExecutionBind => "execution_bind",
            Self::Dispatch => "dispatch",
            Self::OutcomeObserve => "outcome_observe",
            Self::DossierRead => "dossier_read",
            Self::DecisionInsert => "decision_insert",
            Self::ContextPackInsert => "context_pack_insert",
            Self::UsageWrite => "usage_write",
            Self::WorkspaceContinuation => "workspace_continuation",
            Self::EventWrite => "event_write",
            Self::TerminalTransition => "terminal_transition",
        }
    }
}

#[derive(Clone, Default)]
pub struct FaultPlan {
    faults: Arc<Mutex<HashMap<(FaultBoundary, FaultKind), u64>>>,
}

impl FaultPlan {
    /// Create an empty fault plan with no faults injected (production default).
    pub fn new() -> Self {
        Self::default()
    }

    /// Inject a fault that triggers on the nth call (1-indexed).
    /// FailNthCall requires the count; other kinds use count=1.
    pub fn inject(&self, boundary: FaultBoundary, kind: FaultKind, nth: u64) {
        self.faults.lock().unwrap().insert((boundary, kind), nth);
    }

    /// Inject a fault that triggers on the next call.
    pub fn inject_once(&self, boundary: FaultBoundary, kind: FaultKind) {
        self.inject(boundary, kind, 1);
    }

    /// Check if a fault should trigger for this call. Consumes the fault
    /// (one-shot) on match, so a resumed run proceeds normally.
    pub fn check(&self, boundary: FaultBoundary, call_count: &mut u64) -> Option<FaultKind> {
        *call_count += 1;
        let key = (boundary, FaultKind::FailNthCall);
        let mut m = self.faults.lock().unwrap();
        if let Some(nth) = m.get(&key) {
            if *call_count == *nth {
                m.remove(&key);
                return Some(FaultKind::FailNthCall);
            }
        }
        // Collect the kind with remaining count first, then mutate.
        let mut found: Option<(FaultKind, u64)> = None;
        for kind in &[
            FaultKind::FailBeforeEffect,
            FaultKind::FailAfterEffectBeforeResponse,
            FaultKind::ResponseLostAfterSuccess,
            FaultKind::CrashAfterDurableWrite,
            FaultKind::OwnerTakeover,
            FaultKind::ObservedStateMutation,
        ] {
            let key = (boundary, *kind);
            if let Some(&count) = m.get(&key) {
                if count > 0 {
                    let remaining = count - 1;
                    found = Some((*kind, remaining));
                    break;
                }
            }
        }
        if let Some((kind, remaining)) = found {
            let key = (boundary, kind);
            if remaining == 0 {
                m.remove(&key);
            } else {
                m.insert(key, remaining);
            }
            return Some(kind);
        }
        None
    }
}
