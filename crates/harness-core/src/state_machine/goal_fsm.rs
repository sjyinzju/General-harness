//! GoalFsm — validates Goal lifecycle transitions.
//!
//! Terminal states (Succeeded, Failed, Cancelled) have NO valid successors.
//! Active → Planning creates a new PlanRevision; never overwrites old one.
//! All transitions are validated BEFORE mutation.

use crate::contracts::goal::GoalState;

/// Pure state machine for Goal lifecycle transitions.
pub struct GoalFsm;

impl GoalFsm {
    /// Returns true if `from` can transition to `to`.
    pub fn can_transition(from: GoalState, to: GoalState) -> bool {
        // Terminal states cannot transition
        if from.is_terminal() {
            return false;
        }
        // Cannot transition to self
        if from == to {
            return false;
        }
        // Validate specific transitions
        GoalFsm::is_valid(from, to)
    }

    fn is_valid(from: GoalState, to: GoalState) -> bool {
        use GoalState::*;
        matches!(
            (from, to),
            // Draft progression
            (Draft, Validated)
                | (Draft, Cancelled)
                // Validated progression
                | (Validated, Planning)
                | (Validated, Cancelled)
                // Planning progression
                | (Planning, Active)
                | (Planning, Validated)  // replan from planning
                | (Planning, WaitingForApproval)
                | (Planning, Cancelled)
                | (Planning, Failed)
                // Active progression
                | (Active, Planning)  // replan — creates new PlanRevision
                | (Active, WaitingForApproval)
                | (Active, Paused)
                | (Active, Blocked)
                | (Active, Succeeded)
                | (Active, Failed)
                | (Active, Cancelled)
                // WaitingForApproval progression
                | (WaitingForApproval, Active)
                | (WaitingForApproval, Planning)
                | (WaitingForApproval, Cancelled)
                | (WaitingForApproval, Failed)
                // Paused progression
                | (Paused, Active)
                | (Paused, WaitingForApproval)
                | (Paused, Cancelled)
                | (Paused, Failed)
                // Blocked progression
                | (Blocked, Active)
                | (Blocked, WaitingForApproval)
                | (Blocked, Paused)
                | (Blocked, Failed)
                | (Blocked, Cancelled)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_no_transitions() {
        for terminal in &[
            GoalState::Succeeded,
            GoalState::Failed,
            GoalState::Cancelled,
        ] {
            for target in &[
                GoalState::Draft,
                GoalState::Validated,
                GoalState::Planning,
                GoalState::Active,
                GoalState::WaitingForApproval,
                GoalState::Paused,
                GoalState::Blocked,
                GoalState::Succeeded,
                GoalState::Failed,
                GoalState::Cancelled,
            ] {
                assert!(
                    !GoalFsm::can_transition(*terminal, *target),
                    "terminal {terminal:?} should not transition to {target:?}"
                );
            }
        }
    }

    #[test]
    fn test_draft_progression() {
        assert!(GoalFsm::can_transition(
            GoalState::Draft,
            GoalState::Validated
        ));
        assert!(GoalFsm::can_transition(
            GoalState::Draft,
            GoalState::Cancelled
        ));
        assert!(!GoalFsm::can_transition(
            GoalState::Draft,
            GoalState::Active
        ));
    }

    #[test]
    fn test_validated_progression() {
        assert!(GoalFsm::can_transition(
            GoalState::Validated,
            GoalState::Planning
        ));
        assert!(GoalFsm::can_transition(
            GoalState::Validated,
            GoalState::Cancelled
        ));
        assert!(!GoalFsm::can_transition(
            GoalState::Validated,
            GoalState::Succeeded
        ));
    }

    #[test]
    fn test_active_to_planning_replan() {
        // Active → Planning is allowed for replan (creates new PlanRevision)
        assert!(GoalFsm::can_transition(
            GoalState::Active,
            GoalState::Planning
        ));
    }

    #[test]
    fn test_active_to_succeeded() {
        assert!(GoalFsm::can_transition(
            GoalState::Active,
            GoalState::Succeeded
        ));
    }

    #[test]
    fn test_paused_progression() {
        assert!(GoalFsm::can_transition(
            GoalState::Paused,
            GoalState::Active
        ));
        assert!(GoalFsm::can_transition(
            GoalState::Paused,
            GoalState::Cancelled
        ));
        assert!(!GoalFsm::can_transition(
            GoalState::Paused,
            GoalState::Succeeded
        ));
    }

    #[test]
    fn test_blocked_progression() {
        assert!(GoalFsm::can_transition(
            GoalState::Blocked,
            GoalState::Active
        ));
        assert!(GoalFsm::can_transition(
            GoalState::Blocked,
            GoalState::WaitingForApproval
        ));
    }

    #[test]
    fn test_no_self_transition() {
        assert!(!GoalFsm::can_transition(
            GoalState::Active,
            GoalState::Active
        ));
        assert!(!GoalFsm::can_transition(GoalState::Draft, GoalState::Draft));
    }
}
