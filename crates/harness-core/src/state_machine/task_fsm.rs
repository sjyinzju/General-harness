use crate::contracts::task::TaskLifecycle;

pub struct TaskFsm;

impl TaskFsm {
    /// Returns true if `from → to` is a legal state transition.
    /// Terminal states have NO valid successors.
    pub fn can_transition(from: &TaskLifecycle, to: &TaskLifecycle) -> bool {
        if from.is_terminal() {
            return false;
        }
        matches!(
            (from, to),
            (TaskLifecycle::Pending, TaskLifecycle::Ready)
                | (TaskLifecycle::Pending, TaskLifecycle::Superseded)
                | (TaskLifecycle::Pending, TaskLifecycle::Cancelled)
                | (TaskLifecycle::Ready, TaskLifecycle::Dispatched)
                | (TaskLifecycle::Ready, TaskLifecycle::Cancelled)
                | (TaskLifecycle::Dispatched, TaskLifecycle::Running)
                | (TaskLifecycle::Dispatched, TaskLifecycle::RetryPending) // Agent start failed
                | (TaskLifecycle::Dispatched, TaskLifecycle::Failed)
                | (TaskLifecycle::Running, TaskLifecycle::AwaitingInput)
                | (TaskLifecycle::Running, TaskLifecycle::Submitted)
                // Execution failed but retries remain → RetryPending
                | (TaskLifecycle::Running, TaskLifecycle::RetryPending)
                | (TaskLifecycle::Running, TaskLifecycle::Failed)
                | (TaskLifecycle::AwaitingInput, TaskLifecycle::Running)
                | (TaskLifecycle::AwaitingInput, TaskLifecycle::RetryPending)
                | (TaskLifecycle::AwaitingInput, TaskLifecycle::Failed)
                // RetryPending → reallocate resources → Dispatched
                | (TaskLifecycle::RetryPending, TaskLifecycle::Dispatched)
                | (TaskLifecycle::RetryPending, TaskLifecycle::Failed) // no more retries
                | (TaskLifecycle::RetryPending, TaskLifecycle::Cancelled)
                | (TaskLifecycle::Submitted, TaskLifecycle::Verified)
                | (TaskLifecycle::Submitted, TaskLifecycle::RetryPending) // verification failed
                | (TaskLifecycle::Submitted, TaskLifecycle::Failed)
                | (TaskLifecycle::Verified, TaskLifecycle::Done)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_no_transitions() {
        for terminal in &[
            TaskLifecycle::Done,
            TaskLifecycle::Cancelled,
            TaskLifecycle::Superseded,
            TaskLifecycle::Failed,
        ] {
            assert!(terminal.is_terminal());
            for target in &[
                TaskLifecycle::Pending,
                TaskLifecycle::Ready,
                TaskLifecycle::RetryPending,
            ] {
                assert!(
                    !TaskFsm::can_transition(terminal, target),
                    "{terminal:?} → {target:?} should be illegal"
                );
            }
        }
    }

    #[test]
    fn test_retry_uses_retry_pending() {
        // Running → RetryPending (execution failed, retries remain)
        assert!(TaskFsm::can_transition(
            &TaskLifecycle::Running,
            &TaskLifecycle::RetryPending
        ));
        // RetryPending → Dispatched (new Execution created)
        assert!(TaskFsm::can_transition(
            &TaskLifecycle::RetryPending,
            &TaskLifecycle::Dispatched
        ));
        // Failed is terminal — cannot go back to RetryPending
        assert!(TaskLifecycle::Failed.is_terminal());
        assert!(!TaskFsm::can_transition(
            &TaskLifecycle::Failed,
            &TaskLifecycle::RetryPending
        ));
    }

    #[test]
    fn test_valid_path_to_done() {
        let path = [
            TaskLifecycle::Pending,
            TaskLifecycle::Ready,
            TaskLifecycle::Dispatched,
            TaskLifecycle::Running,
            TaskLifecycle::Submitted,
            TaskLifecycle::Verified,
            TaskLifecycle::Done,
        ];
        for w in path.windows(2) {
            assert!(TaskFsm::can_transition(&w[0], &w[1]));
        }
        assert!(TaskLifecycle::Done.is_terminal());
    }

    #[test]
    fn test_retry_path() {
        // Simulate: Running → execution fails → RetryPending → re-dispatch → Running
        let path = [
            TaskLifecycle::Running,
            TaskLifecycle::RetryPending,
            TaskLifecycle::Dispatched,
            TaskLifecycle::Running,
        ];
        for w in path.windows(2) {
            assert!(TaskFsm::can_transition(&w[0], &w[1]));
        }
        // RetryPending is NOT terminal
        assert!(!TaskLifecycle::RetryPending.is_terminal());
    }
}
