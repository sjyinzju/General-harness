use super::ExecutionLifecycle;

pub struct ExecutionFsm;

impl ExecutionFsm {
    pub fn can_transition(from: &ExecutionLifecycle, to: &ExecutionLifecycle) -> bool {
        if from.is_terminal() && !from.allows_retry() {
            return false;
        }
        matches!(
            (from, to),
            (ExecutionLifecycle::Created, ExecutionLifecycle::Running)
                | (ExecutionLifecycle::Created, ExecutionLifecycle::Failed)
                | (ExecutionLifecycle::Created, ExecutionLifecycle::Cancelled)
                | (ExecutionLifecycle::Running, ExecutionLifecycle::Completed)
                | (ExecutionLifecycle::Running, ExecutionLifecycle::Failed)
                | (ExecutionLifecycle::Running, ExecutionLifecycle::Lost)
                | (ExecutionLifecycle::Running, ExecutionLifecycle::Cancelled)
                // retry
                | (ExecutionLifecycle::Failed, ExecutionLifecycle::Created)
                | (ExecutionLifecycle::Lost, ExecutionLifecycle::Created)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_no_transitions_except_retry() {
        assert!(ExecutionFsm::can_transition(
            &ExecutionLifecycle::Failed,
            &ExecutionLifecycle::Created
        ));
        assert!(ExecutionFsm::can_transition(
            &ExecutionLifecycle::Lost,
            &ExecutionLifecycle::Created
        ));
        assert!(!ExecutionFsm::can_transition(
            &ExecutionLifecycle::Completed,
            &ExecutionLifecycle::Created
        ));
        assert!(!ExecutionFsm::can_transition(
            &ExecutionLifecycle::Cancelled,
            &ExecutionLifecycle::Created
        ));
    }
}
