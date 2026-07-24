//! Commit FSM — state machine for controlled commit creation.
//!
//! Legal transitions:
//!   Requested → Materializing | Cancelled
//!   Materializing → Created | Failed | Cancelled
//!   No transitions from terminal states.
//!
//! Terminal states: Created, Blocked, Failed, Cancelled

use crate::contracts::commit::CommitState;

pub struct CommitFsm;

impl CommitFsm {
    /// Returns true if `from → to` is a legal state transition.
    pub fn can_transition(from: &CommitState, to: &CommitState) -> bool {
        if from.is_terminal() {
            return false;
        }
        // Cancelled is always allowed from any non-terminal state.
        if *to == CommitState::Cancelled {
            return true;
        }
        matches!(
            (from, to),
            (CommitState::Requested, CommitState::Materializing)
                | (CommitState::Materializing, CommitState::Created)
                | (CommitState::Materializing, CommitState::Failed)
                | (CommitState::Requested, CommitState::Blocked)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_no_transitions() {
        for terminal in &[
            CommitState::Created,
            CommitState::Blocked,
            CommitState::Failed,
            CommitState::Cancelled,
        ] {
            assert!(terminal.is_terminal());
            for target in &[CommitState::Requested, CommitState::Materializing] {
                assert!(
                    !CommitFsm::can_transition(terminal, target),
                    "Terminal {terminal:?} → {target:?} should be illegal"
                );
            }
        }
    }

    #[test]
    fn test_valid_path_to_created() {
        assert!(CommitFsm::can_transition(
            &CommitState::Requested,
            &CommitState::Materializing
        ));
        assert!(CommitFsm::can_transition(
            &CommitState::Materializing,
            &CommitState::Created
        ));
    }

    #[test]
    fn test_valid_path_to_failed() {
        assert!(CommitFsm::can_transition(
            &CommitState::Requested,
            &CommitState::Materializing
        ));
        assert!(CommitFsm::can_transition(
            &CommitState::Materializing,
            &CommitState::Failed
        ));
    }

    #[test]
    fn test_requested_to_blocked() {
        assert!(CommitFsm::can_transition(
            &CommitState::Requested,
            &CommitState::Blocked
        ));
    }

    #[test]
    fn test_cancelled_from_any_non_terminal() {
        assert!(CommitFsm::can_transition(
            &CommitState::Requested,
            &CommitState::Cancelled
        ));
        assert!(CommitFsm::can_transition(
            &CommitState::Materializing,
            &CommitState::Cancelled
        ));
    }

    #[test]
    fn test_no_skip_materializing() {
        assert!(!CommitFsm::can_transition(
            &CommitState::Requested,
            &CommitState::Created
        ));
    }

    #[test]
    fn test_no_created_to_failed() {
        assert!(!CommitFsm::can_transition(
            &CommitState::Created,
            &CommitState::Failed
        ));
    }
}
