//! Review FSM — state machine for candidate review lifecycle.
//!
//! Legal transitions:
//!   Requested → Preparing | Cancelled
//!   Preparing → Prechecking | Cancelled
//!   Prechecking → Reviewing | Blocked | Cancelled
//!   Reviewing → Approved | Rejected | Blocked | Cancelled
//!   Any non-terminal → Stale (candidate changed)
//!   Any non-terminal → Cancelled
//!
//! Terminal states (no successors): Approved, Rejected, Blocked, Cancelled, Stale

use crate::contracts::review::ReviewState;

pub struct ReviewFsm;

impl ReviewFsm {
    /// Returns true if `from → to` is a legal state transition.
    pub fn can_transition(from: &ReviewState, to: &ReviewState) -> bool {
        if from.is_terminal() {
            return false;
        }
        // Cancelled is always allowed from any non-terminal state.
        if *to == ReviewState::Cancelled {
            return true;
        }
        // Stale is always allowed from any non-terminal state.
        if *to == ReviewState::Stale {
            return true;
        }
        matches!(
            (from, to),
            (ReviewState::Requested, ReviewState::Preparing)
                | (ReviewState::Preparing, ReviewState::Prechecking)
                | (ReviewState::Prechecking, ReviewState::Reviewing)
                | (ReviewState::Prechecking, ReviewState::Blocked)
                | (ReviewState::Reviewing, ReviewState::Approved)
                | (ReviewState::Reviewing, ReviewState::Rejected)
                | (ReviewState::Reviewing, ReviewState::Blocked)
        )
    }

    /// Returns true if the state transition is forward progress
    /// (toward a terminal state, excluding Cancelled/Stale).
    pub fn is_forward_progress(from: &ReviewState, to: &ReviewState) -> bool {
        matches!(
            (from, to),
            (ReviewState::Requested, ReviewState::Preparing)
                | (ReviewState::Preparing, ReviewState::Prechecking)
                | (ReviewState::Prechecking, ReviewState::Reviewing)
                | (ReviewState::Reviewing, ReviewState::Approved)
                | (ReviewState::Reviewing, ReviewState::Rejected)
                | (ReviewState::Prechecking, ReviewState::Blocked)
                | (ReviewState::Reviewing, ReviewState::Blocked)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_no_transitions() {
        for terminal in &[
            ReviewState::Approved,
            ReviewState::Rejected,
            ReviewState::Blocked,
            ReviewState::Cancelled,
            ReviewState::Stale,
        ] {
            assert!(terminal.is_terminal(), "{terminal:?} should be terminal");
            for target in &[
                ReviewState::Requested,
                ReviewState::Preparing,
                ReviewState::Prechecking,
                ReviewState::Reviewing,
            ] {
                assert!(
                    !ReviewFsm::can_transition(terminal, target),
                    "Terminal {terminal:?} → {target:?} should be illegal"
                );
            }
        }
    }

    #[test]
    fn test_valid_path_to_approved() {
        let path = [
            ReviewState::Requested,
            ReviewState::Preparing,
            ReviewState::Prechecking,
            ReviewState::Reviewing,
            ReviewState::Approved,
        ];
        for w in path.windows(2) {
            assert!(
                ReviewFsm::can_transition(&w[0], &w[1]),
                "{:?} → {:?} should be legal",
                w[0],
                w[1]
            );
        }
        assert!(ReviewState::Approved.is_terminal());
    }

    #[test]
    fn test_valid_path_to_rejected() {
        let path = [
            ReviewState::Requested,
            ReviewState::Preparing,
            ReviewState::Prechecking,
            ReviewState::Reviewing,
            ReviewState::Rejected,
        ];
        for w in path.windows(2) {
            assert!(ReviewFsm::can_transition(&w[0], &w[1]));
        }
        assert!(ReviewState::Rejected.is_terminal());
    }

    #[test]
    fn test_precheck_blocked() {
        assert!(ReviewFsm::can_transition(
            &ReviewState::Prechecking,
            &ReviewState::Blocked
        ));
        assert!(ReviewState::Blocked.is_terminal());
    }

    #[test]
    fn test_reviewing_blocked() {
        assert!(ReviewFsm::can_transition(
            &ReviewState::Reviewing,
            &ReviewState::Blocked
        ));
    }

    #[test]
    fn test_cancelled_from_any_non_terminal() {
        for state in &[
            ReviewState::Requested,
            ReviewState::Preparing,
            ReviewState::Prechecking,
            ReviewState::Reviewing,
        ] {
            assert!(
                ReviewFsm::can_transition(state, &ReviewState::Cancelled),
                "{state:?} → Cancelled should be legal"
            );
        }
    }

    #[test]
    fn test_stale_from_any_non_terminal() {
        for state in &[
            ReviewState::Requested,
            ReviewState::Preparing,
            ReviewState::Prechecking,
            ReviewState::Reviewing,
        ] {
            assert!(
                ReviewFsm::can_transition(state, &ReviewState::Stale),
                "{state:?} → Stale should be legal"
            );
        }
    }

    #[test]
    fn test_illegal_skip_precheck() {
        // Requested → Reviewing is NOT allowed (must go through Preparing + Prechecking)
        assert!(!ReviewFsm::can_transition(
            &ReviewState::Requested,
            &ReviewState::Reviewing
        ));
    }

    #[test]
    fn test_illegal_skip_preparing() {
        // Requested → Prechecking is NOT allowed
        assert!(!ReviewFsm::can_transition(
            &ReviewState::Requested,
            &ReviewState::Prechecking
        ));
    }

    #[test]
    fn test_illegal_approved_to_rejected() {
        assert!(!ReviewFsm::can_transition(
            &ReviewState::Approved,
            &ReviewState::Rejected
        ));
    }

    #[test]
    fn test_illegal_rejected_to_approved() {
        assert!(!ReviewFsm::can_transition(
            &ReviewState::Rejected,
            &ReviewState::Approved
        ));
    }

    #[test]
    fn test_re_review_requires_new_review_request() {
        // Terminal Approved → Requested is illegal (must create new ReviewRequest)
        assert!(!ReviewFsm::can_transition(
            &ReviewState::Approved,
            &ReviewState::Requested
        ));
        assert!(!ReviewFsm::can_transition(
            &ReviewState::Rejected,
            &ReviewState::Requested
        ));
        assert!(!ReviewFsm::can_transition(
            &ReviewState::Blocked,
            &ReviewState::Requested
        ));
    }

    #[test]
    fn test_no_implicit_stale_transition_from_terminal() {
        // Once terminal, even Stale is illegal (already decided)
        assert!(!ReviewFsm::can_transition(
            &ReviewState::Approved,
            &ReviewState::Stale
        ));
    }

    #[test]
    fn test_forward_progress() {
        assert!(ReviewFsm::is_forward_progress(
            &ReviewState::Requested,
            &ReviewState::Preparing
        ));
        assert!(!ReviewFsm::is_forward_progress(
            &ReviewState::Reviewing,
            &ReviewState::Cancelled
        ));
        assert!(!ReviewFsm::is_forward_progress(
            &ReviewState::Reviewing,
            &ReviewState::Stale
        ));
    }
}
