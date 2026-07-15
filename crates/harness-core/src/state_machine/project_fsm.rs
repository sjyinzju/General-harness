use crate::contracts::project::ProjectLifecycle;

pub struct ProjectFsm;

impl ProjectFsm {
    pub fn can_transition(from: &ProjectLifecycle, to: &ProjectLifecycle) -> bool {
        if from.is_terminal() {
            return false;
        }
        matches!(
            (from, to),
            (ProjectLifecycle::Created, ProjectLifecycle::Clarifying)
                | (ProjectLifecycle::Clarifying, ProjectLifecycle::GoalLocked)
                | (ProjectLifecycle::GoalLocked, ProjectLifecycle::Planning)
                | (
                    ProjectLifecycle::Planning,
                    ProjectLifecycle::AwaitingApproval
                )
                | (ProjectLifecycle::AwaitingApproval, ProjectLifecycle::Active)
                | (
                    ProjectLifecycle::AwaitingApproval,
                    ProjectLifecycle::Planning
                )
                | (
                    ProjectLifecycle::AwaitingApproval,
                    ProjectLifecycle::Cancelled
                )
                | (ProjectLifecycle::Active, ProjectLifecycle::Integrating)
                | (ProjectLifecycle::Active, ProjectLifecycle::Failed)
                | (ProjectLifecycle::Active, ProjectLifecycle::Cancelled)
                | (ProjectLifecycle::Integrating, ProjectLifecycle::Verifying)
                | (ProjectLifecycle::Integrating, ProjectLifecycle::Active)
                | (ProjectLifecycle::Integrating, ProjectLifecycle::Cancelled)
                | (ProjectLifecycle::Verifying, ProjectLifecycle::Delivering)
                | (ProjectLifecycle::Verifying, ProjectLifecycle::Active)
                | (ProjectLifecycle::Verifying, ProjectLifecycle::Cancelled)
                | (ProjectLifecycle::Delivering, ProjectLifecycle::Done)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_no_transitions() {
        for terminal in &[
            ProjectLifecycle::Done,
            ProjectLifecycle::Cancelled,
            ProjectLifecycle::Failed,
        ] {
            for target in &[
                ProjectLifecycle::Created,
                ProjectLifecycle::Active,
                ProjectLifecycle::Done,
            ] {
                assert!(!ProjectFsm::can_transition(terminal, target));
            }
        }
    }

    #[test]
    fn test_valid_path_to_done() {
        let path = [
            ProjectLifecycle::Created,
            ProjectLifecycle::Clarifying,
            ProjectLifecycle::GoalLocked,
            ProjectLifecycle::Planning,
            ProjectLifecycle::AwaitingApproval,
            ProjectLifecycle::Active,
            ProjectLifecycle::Integrating,
            ProjectLifecycle::Verifying,
            ProjectLifecycle::Delivering,
            ProjectLifecycle::Done,
        ];
        for w in path.windows(2) {
            assert!(ProjectFsm::can_transition(&w[0], &w[1]));
        }
    }
}
