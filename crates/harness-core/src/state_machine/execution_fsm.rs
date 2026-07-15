use crate::state_machine::ExecutionLifecycle;

pub struct ExecutionFsm;

impl ExecutionFsm {
    /// Terminal states have NO valid successor transitions.
    /// A "retry" creates a NEW Execution Attempt — it does NOT
    /// transition an existing Failed/Lost Execution.
    pub fn can_transition(from: &ExecutionLifecycle, to: &ExecutionLifecycle) -> bool {
        if from.is_terminal() {
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
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_terminal_no_transitions() {
        for terminal in &[
            ExecutionLifecycle::Completed,
            ExecutionLifecycle::Failed,
            ExecutionLifecycle::Lost,
            ExecutionLifecycle::Cancelled,
        ] {
            assert!(terminal.is_terminal(), "{terminal:?} should be terminal");
            for target in &[ExecutionLifecycle::Created, ExecutionLifecycle::Running] {
                assert!(
                    !ExecutionFsm::can_transition(terminal, target),
                    "Terminal {terminal:?} → {target:?} should be illegal"
                );
            }
        }
    }

    #[test]
    fn test_retry_creates_new_execution() {
        // Failed is terminal — cannot transition to Created
        assert!(!ExecutionFsm::can_transition(
            &ExecutionLifecycle::Failed,
            &ExecutionLifecycle::Created
        ));
        // Lost is terminal — cannot transition to Created
        assert!(!ExecutionFsm::can_transition(
            &ExecutionLifecycle::Lost,
            &ExecutionLifecycle::Created
        ));
        // But the Task can create a brand-new Execution:
        let new_exec = ExecutionLifecycle::Created;
        assert!(!new_exec.is_terminal());
        assert!(ExecutionFsm::can_transition(
            &new_exec,
            &ExecutionLifecycle::Running
        ));
    }
}
