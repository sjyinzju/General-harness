use crate::contracts::task::TaskLifecycle;

pub struct TaskFsm;

impl TaskFsm {
    pub fn can_transition(from: &TaskLifecycle, to: &TaskLifecycle) -> bool {
        if from.is_terminal() && !from.allows_retry() {
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
                | (TaskLifecycle::Dispatched, TaskLifecycle::Pending)
                | (TaskLifecycle::Dispatched, TaskLifecycle::Failed)
                | (TaskLifecycle::Running, TaskLifecycle::AwaitingInput)
                | (TaskLifecycle::Running, TaskLifecycle::Submitted)
                | (TaskLifecycle::Running, TaskLifecycle::Pending)
                | (TaskLifecycle::Running, TaskLifecycle::Failed)
                | (TaskLifecycle::AwaitingInput, TaskLifecycle::Running)
                | (TaskLifecycle::AwaitingInput, TaskLifecycle::Failed)
                | (TaskLifecycle::Submitted, TaskLifecycle::Verified)
                | (TaskLifecycle::Submitted, TaskLifecycle::Pending)
                | (TaskLifecycle::Verified, TaskLifecycle::Done)
                // retry from terminal
                | (TaskLifecycle::Failed, TaskLifecycle::Pending)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_no_transitions() {
        // Done, Cancelled, Superseded: no transitions (even to Pending)
        for terminal in &[
            TaskLifecycle::Done,
            TaskLifecycle::Cancelled,
            TaskLifecycle::Superseded,
        ] {
            assert!(!TaskFsm::can_transition(terminal, &TaskLifecycle::Pending));
            assert!(!TaskFsm::can_transition(terminal, &TaskLifecycle::Ready));
        }
        // Failed allows retry → Pending
        assert!(TaskFsm::can_transition(
            &TaskLifecycle::Failed,
            &TaskLifecycle::Pending
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
    }
}
