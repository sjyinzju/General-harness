//! Render entry point and TestBackend render tests.
//!
//! Rendering is a pure projection: `draw(f, state)` never mutates state and
//! never performs IPC — tests drive it with `TestBackend` and inspect the
//! buffer directly.

use super::state::TuiAppState;
use super::widgets;

/// Draw the current state onto any ratatui frame.
pub fn draw(f: &mut ratatui::Frame, state: &TuiAppState) {
    widgets::draw(f, state);
}

// ── Render tests ───────────────────────────────────────────────────────

#[cfg(test)]
pub(crate) mod testutil {
    use super::*;
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;
    use ratatui::Terminal;

    /// Render `state` into a TestBackend buffer.
    pub fn render(state: &TuiAppState) -> Buffer {
        let mut term = Terminal::new(TestBackend::new(state.term_cols, state.term_rows))
            .expect("test backend");
        term.draw(|f| draw(f, state)).expect("draw");
        term.backend().buffer().clone()
    }

    /// True if the buffer shows `needle` anywhere.
    pub fn contains(buf: &Buffer, needle: &str) -> bool {
        let width = buf.area.width as usize;
        for y in 0..buf.area.height as usize {
            let row: String = (0..width)
                .map(|x| {
                    buf.cell((x as u16, y as u16))
                        .map(|c| c.symbol())
                        .unwrap_or(" ")
                })
                .collect::<Vec<_>>()
                .join("");
            if row.contains(needle) {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::{contains, render};
    use super::*;
    use crate::tui::reducer::reduce;
    use crate::tui::state::{ConversationEntry, GoalListItem};
    use harness_core::contracts::presentation::{
        GoalSnapshot, PendingInteraction, RunningActivity, SnapshotGoal, SnapshotPlan,
        SnapshotTask, UsageSummary,
    };
    use serde_json::json;

    fn base_state() -> TuiAppState {
        let mut s = TuiAppState::new("demo-project");
        s.term_cols = 100;
        s.term_rows = 32;
        s
    }

    fn snap(state: &str) -> GoalSnapshot {
        GoalSnapshot {
            goal: SnapshotGoal {
                goal_id: "goal-1".into(),
                revision: 1,
                title: "Ship the widget".into(),
                objective: "Build it".into(),
                state: state.into(),
                budget: json!({}),
                approval_policy: json!({}),
                created_at: "t".into(),
                updated_at: "t".into(),
            },
            active_plan: Some(SnapshotPlan {
                plan_revision_id: "pr-1".into(),
                revision_number: 1,
                state: "active".into(),
            }),
            latest_plan: None,
            tasks: vec![
                SnapshotTask {
                    planned_task_id: "pt-1".into(),
                    milestone_id: "m-1".into(),
                    client_ref: "T1".into(),
                    title: "Design widget".into(),
                    state: "completed".into(),
                    dependencies: vec![],
                    risk_level: "low".into(),
                    requires_approval: false,
                    expected_evidence: vec![],
                    materialized_task_id: None,
                    materialized_loop_id: None,
                    agent_kind: None,
                    model: None,
                    provider: None,
                },
                SnapshotTask {
                    planned_task_id: "pt-2".into(),
                    milestone_id: "m-1".into(),
                    client_ref: "T2".into(),
                    title: "Implement widget".into(),
                    state: "running".into(),
                    dependencies: vec!["T1".into()],
                    risk_level: "medium".into(),
                    requires_approval: false,
                    expected_evidence: vec![],
                    materialized_task_id: None,
                    materialized_loop_id: None,
                    agent_kind: Some("claude-code".into()),
                    model: Some("sonnet-4".into()),
                    provider: None,
                },
            ],
            pending_interactions: vec![],
            interventions: vec![],
            running_activities: vec![],
            usage: UsageSummary::default(),
            last_event_sequence: 3,
        }
    }

    fn attached(state: &str) -> TuiAppState {
        let mut s = base_state();
        reduce(
            &mut s,
            crate::tui::action::TuiAction::SnapshotReceived {
                snapshot: Box::new(snap(state)),
            },
        );
        s
    }

    #[test]
    fn empty_state_shows_shell_and_new_goal_prompt() {
        let buf = render(&base_state());
        assert!(contains(&buf, "General Harness"));
        assert!(contains(&buf, "demo-project"));
        assert!(contains(&buf, "> goal"));
        assert!(contains(&buf, "no goal attached"));
    }

    #[test]
    fn too_small_terminal_shows_notice_not_panic() {
        let mut s = base_state();
        s.term_cols = 40;
        s.term_rows = 10;
        let buf = render(&s);
        assert!(contains(&buf, "Terminal too small"));
        assert!(contains(&buf, "60"));
    }

    #[test]
    fn running_goal_renders_tasks_and_activity() {
        let mut s = attached("active");
        s.term_cols = 300; // wide enough that the activity row is not wrapped
        s.snapshot.as_mut().unwrap().running_activities = vec![RunningActivity {
            run_id: "run-1".into(),
            state: "running".into(),
            iteration_number: 2,
            plan_revision_id: Some("pr-1".into()),
            task_title: Some("Implement widget".into()),
            agent_kind: Some("claude-code".into()),
            model: None,
        }];
        let buf = render(&s);
        assert!(contains(&buf, "Ship the widget"));
        assert!(contains(&buf, "[active]"));
        assert!(contains(&buf, "T1"));
        assert!(contains(&buf, "Design widget"));
        assert!(contains(&buf, "✓"));
        assert!(contains(&buf, "●"));
        assert!(contains(&buf, "iteration 2"));
        // Real runtime assignment is displayed where available (§67)…
        assert!(contains(&buf, "claude-code"));
        assert!(contains(&buf, "sonnet-4"));
        // …and absent values are shown honestly, never fabricated.
        assert!(contains(&buf, "agent: claude-code · model: unknown"));
    }

    #[test]
    fn planning_state_renders_panels() {
        let mut s = attached("planning");
        s.focus = crate::tui::state::Focus::Tasks; // focused title is uppercase
        let buf = render(&s);
        assert!(contains(&buf, "[planning]"));
        assert!(contains(&buf, "PLAN / TASKS"));
    }

    #[test]
    fn clarification_modal_renders_current_question() {
        let mut s = base_state();
        let mut snapshot = snap("active");
        snapshot.pending_interactions = vec![PendingInteraction {
            approval_id: "ap-1".into(),
            kind: "provide_missing_information".into(),
            plan_revision_id: None,
            reason: "missing info".into(),
            requested_action: json!({"questions":[
                {"question_id":"q1","prompt":"Which database?","choices":["sqlite","postgres"],"required":true}
            ]}),
            created_at: "t".into(),
        }];
        reduce(
            &mut s,
            crate::tui::action::TuiAction::SnapshotReceived {
                snapshot: Box::new(snapshot),
            },
        );
        let buf = render(&s);
        assert!(contains(&buf, "clarification needed"));
        assert!(contains(&buf, "Which database?"));
        assert!(contains(&buf, "sqlite | postgres"));
        assert!(contains(&buf, "> answer"));
    }

    #[test]
    fn plan_approval_modal_shows_decision_keys() {
        let mut s = base_state();
        let mut snapshot = snap("waiting_for_approval");
        snapshot.pending_interactions = vec![PendingInteraction {
            approval_id: "ap-9".into(),
            kind: "approve_initial_plan".into(),
            plan_revision_id: Some("pr-7".into()),
            reason: "initial plan".into(),
            requested_action: json!({"revision_number":2,"task_count":2}),
            created_at: "t".into(),
        }];
        reduce(
            &mut s,
            crate::tui::action::TuiAction::SnapshotReceived {
                snapshot: Box::new(snapshot),
            },
        );
        let buf = render(&s);
        assert!(contains(&buf, "plan approval"));
        assert!(contains(&buf, "Plan revision 2"));
        assert!(contains(&buf, "[a] approve"));
        assert!(contains(&buf, "[x] reject"));
    }

    #[test]
    fn paused_state_shows_paused_label() {
        let buf = render(&attached("paused"));
        assert!(contains(&buf, "[paused]"));
        assert!(contains(&buf, "paused"));
    }

    #[test]
    fn failed_state_renders() {
        let buf = render(&attached("failed"));
        assert!(contains(&buf, "[failed]"));
    }

    #[test]
    fn completed_state_renders() {
        let buf = render(&attached("succeeded"));
        assert!(contains(&buf, "[succeeded]"));
        assert!(contains(&buf, "completed"));
    }

    #[test]
    fn disconnected_status_is_visible() {
        let mut s = base_state();
        s.connection = crate::tui::state::ConnectionStatus::Disconnected;
        let buf = render(&s);
        assert!(contains(&buf, "disconnected"));
    }

    #[test]
    fn help_overlay_lists_commands() {
        let mut s = base_state();
        s.pending.help_open = true;
        let buf = render(&s);
        assert!(contains(&buf, "/help"));
        assert!(contains(&buf, "/quit"));
        assert!(contains(&buf, "KEEPS RUNNING"));
    }

    #[test]
    fn usage_unknown_is_not_fabricated() {
        let mut s = attached("active");
        // usage.usage_known is false by default → activity panel shows no numbers.
        let buf = render(&s);
        assert!(!contains(&buf, "usage: in="));
        // And /usage reports honestly.
        for c in "/usage".chars() {
            reduce(&mut s, crate::tui::action::TuiAction::CharInput(c));
        }
        reduce(&mut s, crate::tui::action::TuiAction::Submit);
        let buf = render(&s);
        assert!(contains(&buf, "never fabricated"));
    }

    #[test]
    fn conversation_renders_roles_and_toast_renders_footer() {
        let mut s = base_state();
        s.conversation.push(ConversationEntry::new("You", "hello"));
        s.conversation
            .push(ConversationEntry::new("Harness", "world"));
        s.toast = Some("something happened".into());
        let buf = render(&s);
        assert!(contains(&buf, "You: hello"));
        assert!(contains(&buf, "Harness: world"));
        assert!(contains(&buf, "something happened"));
    }

    #[test]
    fn goals_list_renders_via_conversation() {
        let mut s = base_state();
        reduce(
            &mut s,
            crate::tui::action::TuiAction::GoalsListed {
                goals: vec![GoalListItem {
                    goal_id: "goal-77".into(),
                    title: "Earlier goal".into(),
                    state: "active".into(),
                    created_at: "t1".into(),
                    updated_at: "t2".into(),
                }],
            },
        );
        let buf = render(&s);
        assert!(contains(&buf, "goal-77"));
    }
}
