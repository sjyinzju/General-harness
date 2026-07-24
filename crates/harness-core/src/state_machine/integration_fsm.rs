//! Integration FSM — state machine for integration queue lifecycle.
//!
//! Legal transitions:
//!   Queued → WaitingForLease | Cancelled
//!   WaitingForLease → Preparing | Cancelled
//!   Preparing → Applying | Blocked | Failed | Cancelled
//!   Applying → Verifying | Conflict | Stale | Failed | Cancelled
//!   Verifying → ReadyToPublish | Failed | Cancelled
//!   ReadyToPublish → Integrated | Failed | Cancelled
//!
//! Terminal states: Integrated, Conflict, Blocked, Failed, Cancelled, Stale

use crate::contracts::integration::IntegrationState;

pub struct IntegrationFsm;

impl IntegrationFsm {
    /// Returns true if `from → to` is a legal state transition.
    pub fn can_transition(from: &IntegrationState, to: &IntegrationState) -> bool {
        if from.is_terminal() {
            return false;
        }
        // Cancelled is always allowed from any non-terminal state.
        if *to == IntegrationState::Cancelled {
            return true;
        }
        // Failed can be reached from any non-terminal state.
        if *to == IntegrationState::Failed {
            return matches!(
                from,
                IntegrationState::Preparing
                    | IntegrationState::Applying
                    | IntegrationState::Verifying
                    | IntegrationState::ReadyToPublish
            );
        }
        matches!(
            (from, to),
            (IntegrationState::Queued, IntegrationState::WaitingForLease)
                | (IntegrationState::WaitingForLease, IntegrationState::Preparing)
                | (IntegrationState::Preparing, IntegrationState::Applying)
                | (IntegrationState::Preparing, IntegrationState::Blocked)
                | (IntegrationState::Applying, IntegrationState::Verifying)
                | (IntegrationState::Applying, IntegrationState::Conflict)
                | (IntegrationState::Applying, IntegrationState::Stale)
                | (IntegrationState::Verifying, IntegrationState::ReadyToPublish)
                | (IntegrationState::ReadyToPublish, IntegrationState::Integrated)
        )
    }

    /// Returns true if the state transition is forward progress.
    pub fn is_forward_progress(from: &IntegrationState, to: &IntegrationState) -> bool {
        matches!(
            (from, to),
            (IntegrationState::Queued, IntegrationState::WaitingForLease)
                | (IntegrationState::WaitingForLease, IntegrationState::Preparing)
                | (IntegrationState::Preparing, IntegrationState::Applying)
                | (IntegrationState::Applying, IntegrationState::Verifying)
                | (IntegrationState::Verifying, IntegrationState::ReadyToPublish)
                | (IntegrationState::ReadyToPublish, IntegrationState::Integrated)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_no_transitions() {
        for terminal in &[
            IntegrationState::Integrated,
            IntegrationState::Conflict,
            IntegrationState::Blocked,
            IntegrationState::Failed,
            IntegrationState::Cancelled,
            IntegrationState::Stale,
        ] {
            assert!(terminal.is_terminal());
            for target in &[
                IntegrationState::Queued,
                IntegrationState::WaitingForLease,
                IntegrationState::Preparing,
            ] {
                assert!(
                    !IntegrationFsm::can_transition(terminal, target),
                    "Terminal {terminal:?} → {target:?} should be illegal"
                );
            }
        }
    }

    #[test]
    fn test_valid_path_to_integrated() {
        let path = [
            IntegrationState::Queued,
            IntegrationState::WaitingForLease,
            IntegrationState::Preparing,
            IntegrationState::Applying,
            IntegrationState::Verifying,
            IntegrationState::ReadyToPublish,
            IntegrationState::Integrated,
        ];
        for w in path.windows(2) {
            assert!(
                IntegrationFsm::can_transition(&w[0], &w[1]),
                "{:?} → {:?} should be legal",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn test_applying_to_conflict() {
        assert!(IntegrationFsm::can_transition(
            &IntegrationState::Applying,
            &IntegrationState::Conflict
        ));
    }

    #[test]
    fn test_applying_to_stale() {
        assert!(IntegrationFsm::can_transition(
            &IntegrationState::Applying,
            &IntegrationState::Stale
        ));
    }

    #[test]
    fn test_cancelled_from_any_non_terminal() {
        for state in &[
            IntegrationState::Queued,
            IntegrationState::WaitingForLease,
            IntegrationState::Preparing,
            IntegrationState::Applying,
            IntegrationState::Verifying,
            IntegrationState::ReadyToPublish,
        ] {
            assert!(
                IntegrationFsm::can_transition(state, &IntegrationState::Cancelled),
                "{state:?} → Cancelled should be legal"
            );
        }
    }

    #[test]
    fn test_illegal_queued_to_applying() {
        assert!(!IntegrationFsm::can_transition(
            &IntegrationState::Queued,
            &IntegrationState::Applying
        ));
    }

    #[test]
    fn test_illegal_integrated_to_queued() {
        assert!(!IntegrationFsm::can_transition(
            &IntegrationState::Integrated,
            &IntegrationState::Queued
        ));
    }
}
