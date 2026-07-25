//! PlanFsm — validates PlanRevision lifecycle transitions.
//!
//! Terminal states (Superseded, Completed, Rejected, Invalid, Cancelled)
//! have NO valid successors. Old plan revisions are never overwritten.

use crate::contracts::plan::PlanState;

/// Pure state machine for PlanRevision lifecycle transitions.
pub struct PlanFsm;

impl PlanFsm {
    /// Returns true if `from` can transition to `to`.
    pub fn can_transition(from: PlanState, to: PlanState) -> bool {
        if from.is_terminal() {
            return false;
        }
        if from == to {
            return false;
        }
        PlanFsm::is_valid(from, to)
    }

    fn is_valid(from: PlanState, to: PlanState) -> bool {
        use PlanState::*;
        matches!(
            (from, to),
            // Proposed progression
            (Proposed, Validating)
                | (Proposed, Rejected)
                | (Proposed, Cancelled)
            // Validating progression
            | (Validating, Validated)
                | (Validating, Invalid)
                | (Validating, Rejected)
                | (Validating, Cancelled)
            // Validated progression
            | (Validated, Active)
                | (Validated, Rejected)
                | (Validated, Cancelled)
            // Active progression
            | (Active, Superseded)
                | (Active, Completed)
                | (Active, Cancelled)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_no_transitions() {
        for terminal in &[
            PlanState::Superseded,
            PlanState::Completed,
            PlanState::Rejected,
            PlanState::Invalid,
            PlanState::Cancelled,
        ] {
            for target in &[
                PlanState::Proposed,
                PlanState::Validating,
                PlanState::Validated,
                PlanState::Active,
                PlanState::Superseded,
                PlanState::Completed,
                PlanState::Rejected,
                PlanState::Invalid,
                PlanState::Cancelled,
            ] {
                assert!(
                    !PlanFsm::can_transition(*terminal, *target),
                    "terminal {terminal:?} should not transition to {target:?}"
                );
            }
        }
    }

    #[test]
    fn test_proposed_progression() {
        assert!(PlanFsm::can_transition(PlanState::Proposed, PlanState::Validating));
        assert!(PlanFsm::can_transition(PlanState::Proposed, PlanState::Rejected));
        assert!(PlanFsm::can_transition(PlanState::Proposed, PlanState::Cancelled));
        assert!(!PlanFsm::can_transition(PlanState::Proposed, PlanState::Active));
    }

    #[test]
    fn test_validating_progression() {
        assert!(PlanFsm::can_transition(PlanState::Validating, PlanState::Validated));
        assert!(PlanFsm::can_transition(PlanState::Validating, PlanState::Invalid));
        assert!(!PlanFsm::can_transition(PlanState::Validating, PlanState::Active));
    }

    #[test]
    fn test_validated_progression() {
        assert!(PlanFsm::can_transition(PlanState::Validated, PlanState::Active));
        assert!(PlanFsm::can_transition(PlanState::Validated, PlanState::Rejected));
        assert!(!PlanFsm::can_transition(PlanState::Validated, PlanState::Superseded));
    }

    #[test]
    fn test_active_progression() {
        assert!(PlanFsm::can_transition(PlanState::Active, PlanState::Superseded));
        assert!(PlanFsm::can_transition(PlanState::Active, PlanState::Completed));
        assert!(PlanFsm::can_transition(PlanState::Active, PlanState::Cancelled));
        assert!(!PlanFsm::can_transition(PlanState::Active, PlanState::Validating));
    }

    #[test]
    fn test_no_self_transition() {
        assert!(!PlanFsm::can_transition(PlanState::Active, PlanState::Active));
        assert!(!PlanFsm::can_transition(PlanState::Proposed, PlanState::Proposed));
    }
}
