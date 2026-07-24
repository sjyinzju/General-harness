//! Supervisor lifecycle state machine — validates all state transitions.
//!
//! The FSM enforces:
//! - Terminal states (Stopped, Failed) cannot be overwritten.
//! - Only valid transitions are accepted.
//! - All invalid transitions are rejected with a descriptive error.

use harness_core::contracts::supervisor::SupervisorState;

/// Valid state transitions for the Supervisor lifecycle.
///
/// ```text
/// Created → Starting → AcquiringOwnership → Recovering → Ready
///                                                           ↓
///                                                         Draining → Stopping → Stopped
/// Starting/Recovering/Ready/Draining → Failed
/// (Any) → TakingOver → Recovering (takeover path)
/// ```
pub struct LifecycleFsm;

impl Default for LifecycleFsm {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleFsm {
    pub fn new() -> Self {
        Self
    }

    /// Validate a state transition. Returns `Ok(())` if valid,
    /// or `Err(msg)` if the transition is illegal.
    pub fn validate_transition(
        &self,
        current: SupervisorState,
        next: SupervisorState,
    ) -> Result<(), String> {
        // Terminal states cannot be changed
        if current.is_terminal() {
            return Err(format!(
                "cannot transition from terminal state '{}' to '{}'",
                current, next
            ));
        }

        // Same-state transitions are no-ops (not errors)
        if current == next {
            return Ok(());
        }

        match (current, next) {
            // Normal startup path
            (SupervisorState::Created, SupervisorState::Starting) => Ok(()),
            (SupervisorState::Starting, SupervisorState::AcquiringOwnership) => Ok(()),
            (SupervisorState::AcquiringOwnership, SupervisorState::Recovering) => Ok(()),
            (SupervisorState::AcquiringOwnership, SupervisorState::TakingOver) => Ok(()),
            (SupervisorState::Recovering, SupervisorState::Ready) => Ok(()),

            // Takeover path
            (SupervisorState::TakingOver, SupervisorState::Recovering) => Ok(()),

            // Shutdown path
            (SupervisorState::Ready, SupervisorState::Draining) => Ok(()),
            (SupervisorState::Draining, SupervisorState::Stopping) => Ok(()),
            (SupervisorState::Stopping, SupervisorState::Stopped) => Ok(()),

            // Failure paths (any active state can fail)
            (SupervisorState::Starting, SupervisorState::Failed) => Ok(()),
            (SupervisorState::AcquiringOwnership, SupervisorState::Failed) => Ok(()),
            (SupervisorState::Recovering, SupervisorState::Failed) => Ok(()),
            (SupervisorState::Ready, SupervisorState::Failed) => Ok(()),
            (SupervisorState::Draining, SupervisorState::Failed) => Ok(()),
            (SupervisorState::Stopping, SupervisorState::Failed) => Ok(()),
            (SupervisorState::TakingOver, SupervisorState::Failed) => Ok(()),

            // Direct drain from Ready
            (SupervisorState::Ready, SupervisorState::Stopping) => Ok(()),

            _ => Err(format!(
                "illegal state transition: '{}' → '{}'",
                current, next
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal_startup_path() {
        let fsm = LifecycleFsm::new();
        assert!(fsm
            .validate_transition(SupervisorState::Created, SupervisorState::Starting)
            .is_ok());
        assert!(fsm
            .validate_transition(
                SupervisorState::Starting,
                SupervisorState::AcquiringOwnership
            )
            .is_ok());
        assert!(fsm
            .validate_transition(
                SupervisorState::AcquiringOwnership,
                SupervisorState::Recovering
            )
            .is_ok());
        assert!(fsm
            .validate_transition(SupervisorState::Recovering, SupervisorState::Ready)
            .is_ok());
    }

    #[test]
    fn test_shutdown_path() {
        let fsm = LifecycleFsm::new();
        assert!(fsm
            .validate_transition(SupervisorState::Ready, SupervisorState::Draining)
            .is_ok());
        assert!(fsm
            .validate_transition(SupervisorState::Draining, SupervisorState::Stopping)
            .is_ok());
        assert!(fsm
            .validate_transition(SupervisorState::Stopping, SupervisorState::Stopped)
            .is_ok());
    }

    #[test]
    fn test_takeover_path() {
        let fsm = LifecycleFsm::new();
        assert!(fsm
            .validate_transition(
                SupervisorState::AcquiringOwnership,
                SupervisorState::TakingOver
            )
            .is_ok());
        assert!(fsm
            .validate_transition(SupervisorState::TakingOver, SupervisorState::Recovering)
            .is_ok());
    }

    #[test]
    fn test_failure_paths() {
        let fsm = LifecycleFsm::new();
        // Any active state can transition to Failed
        for state in &[
            SupervisorState::Starting,
            SupervisorState::AcquiringOwnership,
            SupervisorState::Recovering,
            SupervisorState::Ready,
            SupervisorState::Draining,
            SupervisorState::Stopping,
            SupervisorState::TakingOver,
        ] {
            assert!(
                fsm.validate_transition(*state, SupervisorState::Failed)
                    .is_ok(),
                "{} → Failed should be valid",
                state
            );
        }
    }

    #[test]
    fn test_terminal_states_cannot_change() {
        let fsm = LifecycleFsm::new();
        for terminal in &[SupervisorState::Stopped, SupervisorState::Failed] {
            for next in &[
                SupervisorState::Created,
                SupervisorState::Starting,
                SupervisorState::AcquiringOwnership,
                SupervisorState::Recovering,
                SupervisorState::Ready,
                SupervisorState::Draining,
                SupervisorState::Stopping,
            ] {
                assert!(
                    fsm.validate_transition(*terminal, *next).is_err(),
                    "{} → {} should be illegal",
                    terminal,
                    next
                );
            }
        }
    }

    #[test]
    fn test_same_state_noop() {
        let fsm = LifecycleFsm::new();
        // Non-terminal states allow same-state no-ops
        for state in &[
            SupervisorState::Created,
            SupervisorState::Starting,
            SupervisorState::AcquiringOwnership,
            SupervisorState::Recovering,
            SupervisorState::Ready,
            SupervisorState::Draining,
            SupervisorState::Stopping,
            SupervisorState::TakingOver,
        ] {
            assert!(
                fsm.validate_transition(*state, *state).is_ok(),
                "same-state {} should be ok",
                state
            );
        }
        // Terminal states reject ANY transition including same-state
        for terminal in &[SupervisorState::Stopped, SupervisorState::Failed] {
            assert!(
                fsm.validate_transition(*terminal, *terminal).is_err(),
                "terminal state {} should reject same-state transition",
                terminal
            );
        }
    }

    #[test]
    fn test_illegal_transitions() {
        let fsm = LifecycleFsm::new();
        assert!(fsm
            .validate_transition(SupervisorState::Created, SupervisorState::Ready)
            .is_err());
        assert!(fsm
            .validate_transition(SupervisorState::Starting, SupervisorState::Ready)
            .is_err());
        assert!(fsm
            .validate_transition(SupervisorState::Ready, SupervisorState::Recovering)
            .is_err());
        assert!(fsm
            .validate_transition(SupervisorState::Draining, SupervisorState::Ready)
            .is_err());
        assert!(fsm
            .validate_transition(SupervisorState::Stopped, SupervisorState::Ready)
            .is_err());
    }

    #[test]
    fn test_terminal_state_identity() {
        assert!(SupervisorState::Stopped.is_terminal());
        assert!(SupervisorState::Failed.is_terminal());
        assert!(!SupervisorState::Ready.is_terminal());
        assert!(!SupervisorState::Draining.is_terminal());
        assert!(!SupervisorState::Created.is_terminal());
    }

    #[test]
    fn test_accepts_writes() {
        assert!(SupervisorState::Ready.accepts_writes());
        assert!(!SupervisorState::Draining.accepts_writes());
        assert!(!SupervisorState::Recovering.accepts_writes());
        assert!(!SupervisorState::Failed.accepts_writes());
    }
}
