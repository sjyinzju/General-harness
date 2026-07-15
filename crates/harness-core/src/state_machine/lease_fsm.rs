use crate::contracts::workspace::LeaseLifecycle;

pub struct LeaseFsm;

impl LeaseFsm {
    pub fn can_transition(from: &LeaseLifecycle, to: &LeaseLifecycle) -> bool {
        if from.is_terminal() {
            return false;
        }
        matches!(
            (from, to),
            (LeaseLifecycle::Acquired, LeaseLifecycle::Active)
                | (LeaseLifecycle::Acquired, LeaseLifecycle::Expired)
                | (LeaseLifecycle::Active, LeaseLifecycle::Released)
                | (LeaseLifecycle::Active, LeaseLifecycle::Expired)
        )
    }
}
