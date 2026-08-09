//! Stateless widget drawing — pure functions from `TuiAppState` to frames.
//!
//! Every symbol is paired with a textual label so colorless terminals stay
//! readable. Nothing here performs I/O beyond what ratatui's Frame does.

use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use super::state::{
    task_symbol, ConnectionStatus, Focus, InputMode, TuiAppState, MIN_COLS, MIN_ROWS,
};

/// Draw the full UI for the current state.
pub fn draw(f: &mut Frame, state: &TuiAppState) {
    let area = f.area();
    if area.width < MIN_COLS || area.height < MIN_ROWS {
        draw_too_small(f, area, state);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4), // header (2 content lines + borders)
            Constraint::Min(6),    // main (panels + conversation)
            Constraint::Length(2), // input
            Constraint::Length(1), // footer
        ])
        .split(area);

    draw_header(f, chunks[0], state);

    let main = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(chunks[1]);
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(main[0]);
    draw_tasks_panel(f, columns[0], state);
    draw_activity_panel(f, columns[1], state);
    draw_conversation(f, main[1], state);

    draw_input(f, chunks[2], state);
    draw_footer(f, chunks[3], state);

    draw_modals(f, area, state);
}

fn draw_too_small(f: &mut Frame, area: Rect, state: &TuiAppState) {
    let text = format!(
        "Terminal too small ({}x{})\nNeed at least {}x{}\n\nGeneral Harness — {}\n[{}]",
        state.term_cols,
        state.term_rows,
        MIN_COLS,
        MIN_ROWS,
        state.project_label,
        state.connection.label()
    );
    let p = Paragraph::new(text)
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::ALL).title(" resize "));
    f.render_widget(p, area);
}

fn draw_header(f: &mut Frame, area: Rect, state: &TuiAppState) {
    let conn_style = match state.connection {
        ConnectionStatus::Connected => Style::default().fg(Color::Green),
        ConnectionStatus::Reconnecting => Style::default().fg(Color::Yellow),
        ConnectionStatus::Connecting => Style::default().fg(Color::Yellow),
        ConnectionStatus::Disconnected => Style::default().fg(Color::Red),
    };
    let goal_line = match state.snapshot.as_ref() {
        Some(snap) => {
            let state_style = match snap.goal.state.as_str() {
                "succeeded" => Style::default().fg(Color::Green),
                "failed" | "cancelled" => Style::default().fg(Color::Red),
                "paused" | "blocked" => Style::default().fg(Color::Yellow),
                _ => Style::default().fg(Color::Cyan),
            };
            Line::from(vec![
                Span::raw(format!("{} ", snap.goal.title)),
                Span::styled(format!("[{}]", snap.goal.state), state_style),
                Span::raw(format!("  {}", snap.goal.goal_id)),
            ])
        }
        None => Line::from(Span::styled(
            "no goal attached — type a goal below and press Enter",
            Style::default().add_modifier(Modifier::DIM),
        )),
    };
    let header = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(
                format!(" General Harness — {} ", state.project_label),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(format!("[{}]", state.connection.label()), conn_style),
        ]),
        goal_line,
    ])
    .block(Block::default().borders(Borders::ALL));
    f.render_widget(header, area);
}

fn focus_border<'a>(focused: bool) -> Block<'a> {
    let mut b = Block::default().borders(Borders::ALL);
    if focused {
        b = b.border_style(Style::default().fg(Color::Cyan));
    }
    b
}

fn draw_tasks_panel(f: &mut Frame, area: Rect, state: &TuiAppState) {
    let focused = state.focus == Focus::Tasks;
    let mut lines: Vec<Line> = Vec::new();
    match state.snapshot.as_ref() {
        None => lines.push(Line::from(Span::styled(
            "no plan yet",
            Style::default().add_modifier(Modifier::DIM),
        ))),
        Some(snap) => {
            let plan_label = snap
                .active_plan
                .as_ref()
                .or(snap.latest_plan.as_ref())
                .map(|p| format!("plan rev {} [{}]", p.revision_number, p.state))
                .unwrap_or_else(|| "no plan yet".to_string());
            lines.push(Line::from(Span::styled(
                plan_label,
                Style::default().add_modifier(Modifier::BOLD),
            )));
            for t in &snap.tasks {
                let style = match t.state.as_str() {
                    "completed" => Style::default().fg(Color::Green),
                    "running" => Style::default().fg(Color::Cyan),
                    "failed" => Style::default().fg(Color::Red),
                    _ => Style::default(),
                };
                lines.push(Line::from(vec![
                    Span::styled(format!("{} ", task_symbol(&t.state)), style),
                    Span::styled(
                        format!("{} ", t.client_ref),
                        Style::default().add_modifier(Modifier::DIM),
                    ),
                    Span::raw(t.title.clone()),
                ]));
            }
        }
    }
    let title = if focused {
        " PLAN / TASKS "
    } else {
        " plan / tasks "
    };
    let p = Paragraph::new(lines)
        .block(focus_border(focused).title(title))
        .scroll((state.tasks_scroll as u16, 0));
    f.render_widget(p, area);
}

fn draw_activity_panel(f: &mut Frame, area: Rect, state: &TuiAppState) {
    let focused = state.focus == Focus::Activity;
    let mut lines: Vec<Line> = Vec::new();
    match state.snapshot.as_ref() {
        None => lines.push(Line::from(Span::styled(
            "idle",
            Style::default().add_modifier(Modifier::DIM),
        ))),
        Some(snap) => {
            if snap.running_activities.is_empty() {
                let label = match snap.goal.state.as_str() {
                    "paused" => "paused",
                    "succeeded" => "completed",
                    "failed" => "failed",
                    "cancelled" => "cancelled",
                    "waiting_for_approval" => "waiting for your approval",
                    _ => "idle",
                };
                lines.push(Line::from(label.to_string()));
            }
            for run in &snap.running_activities {
                lines.push(Line::from(vec![
                    Span::styled("● ", Style::default().fg(Color::Cyan)),
                    Span::raw(format!(
                        "iteration {} [{}] run {}",
                        run.iteration_number, run.state, run.run_id
                    )),
                ]));
                if let Some(pr) = &run.plan_revision_id {
                    lines.push(Line::from(Span::styled(
                        format!("    plan revision: {pr}"),
                        Style::default().add_modifier(Modifier::DIM),
                    )));
                }
            }
            // Usage — only what the provider reported; never fabricated.
            if snap.usage.usage_known {
                let t = &snap.usage.totals;
                lines.push(Line::from(Span::styled(
                    format!(
                        "usage: in={} out={} calls={}",
                        opt(t.input_tokens),
                        opt(t.output_tokens),
                        opt(t.tool_calls)
                    ),
                    Style::default().add_modifier(Modifier::DIM),
                )));
            }
        }
    }
    let title = if focused { " ACTIVITY " } else { " activity " };
    let p = Paragraph::new(lines)
        .block(focus_border(focused).title(title))
        .wrap(Wrap { trim: false })
        .scroll((state.activity_scroll as u16, 0));
    f.render_widget(p, area);
}

fn opt(v: Option<i64>) -> String {
    v.map(|n| n.to_string()).unwrap_or_else(|| "—".to_string())
}

fn draw_conversation(f: &mut Frame, area: Rect, state: &TuiAppState) {
    let focused = state.focus == Focus::Conversation;
    let inner_height = area.height.saturating_sub(2) as usize;
    let total = state.conversation.len();
    // conversation_scroll = lines scrolled up from the newest entry.
    let end = total.saturating_sub(state.conversation_scroll).max(1);
    let start = end.saturating_sub(inner_height);
    let lines: Vec<Line> = state.conversation[start..end.min(total)]
        .iter()
        .flat_map(|entry| {
            let style = match entry.role.as_str() {
                "You" => Style::default().fg(Color::Green),
                "Error" => Style::default().fg(Color::Red),
                "Event" => Style::default().fg(Color::Yellow),
                _ => Style::default().fg(Color::White),
            };
            // Wrap multi-line entries visually.
            entry
                .text
                .lines()
                .map(|l| {
                    Line::from(vec![
                        Span::styled(format!("{}: ", entry.role), style),
                        Span::raw(l.to_string()),
                    ])
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let title = if focused {
        " CONVERSATION "
    } else {
        " conversation "
    };
    let p = Paragraph::new(lines).block(focus_border(focused).title(title));
    f.render_widget(p, area);
}

fn draw_input(f: &mut Frame, area: Rect, state: &TuiAppState) {
    let label = state.input_mode.prompt().to_ascii_lowercase();
    let prompt = format!("> {label}");
    let hint = match state.input_mode {
        InputMode::NewGoal | InputMode::Goal => "Enter: submit goal (plan needs your approval)",
        InputMode::Answer => "Enter: record answer",
        InputMode::PlanChanges => "Enter: send change feedback",
        InputMode::Message => "Enter: send intervention · /help for commands",
    };
    // Horizontal fit: keep the tail visible when the line overflows.
    let avail = area.width.saturating_sub(prompt.len() as u16 + 4) as usize;
    let shown = truncate_to_width(&state.input.text(), avail.max(1));
    let mut prompt_spans = vec![
        Span::styled(
            format!("{prompt} "),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(shown),
        Span::styled("▌", Style::default().add_modifier(Modifier::SLOW_BLINK)),
    ];
    if !state.input.is_empty() {
        prompt_spans.push(Span::styled(
            format!(
                " [col {} · char {}/{}]",
                state.input.display_cursor_col(),
                state.input.cursor(),
                state.input.text().chars().count()
            ),
            Style::default().add_modifier(Modifier::DIM),
        ));
    }
    let lines = vec![
        Line::from(prompt_spans),
        Line::from(Span::styled(
            hint.to_string(),
            Style::default().add_modifier(Modifier::DIM),
        )),
    ];
    f.render_widget(Paragraph::new(lines), area);
}

/// Keep the trailing slice of `text` that fits in `max_width` columns.
fn truncate_to_width(text: &str, max_width: usize) -> String {
    use super::input::{char_width, display_width};
    if display_width(text) <= max_width {
        return text.to_string();
    }
    let mut out: Vec<char> = Vec::new();
    let mut width = 0;
    for c in text.chars().rev() {
        let w = char_width(c);
        if width + w > max_width.saturating_sub(1) {
            break;
        }
        width += w;
        out.push(c);
    }
    out.reverse();
    let mut s = String::from("…");
    s.extend(out);
    s
}

fn draw_footer(f: &mut Frame, area: Rect, state: &TuiAppState) {
    let toast = state
        .toast
        .clone()
        .unwrap_or_else(|| "Tab: panels · Esc: clear · Ctrl+C: exit (goal keeps running)".into());
    let style = if state.toast.is_some() {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().add_modifier(Modifier::DIM)
    };
    f.render_widget(Paragraph::new(Line::from(Span::styled(toast, style))), area);
}

/// Modals — precedence: cancel confirm > clarification > plan approval > help.
fn draw_modals(f: &mut Frame, area: Rect, state: &TuiAppState) {
    if state.pending.cancel_confirm {
        centered_box(
            f,
            area,
            50,
            5,
            " cancel goal? ",
            vec![
                Line::from("Cancel the attached goal?"),
                Line::from(""),
                Line::from("[y] yes, cancel via IPC   [n] keep running"),
            ],
        );
        return;
    }
    if let Some(_approval_id) = state.pending.clarify_approval_id.as_ref() {
        let idx = state.pending.clarify_index;
        let mut lines = vec![Line::from(format!(
            "Clarification — question {} of {}",
            idx + 1,
            state.pending.clarify_questions.len()
        ))];
        if let Some(q) = state.pending.clarify_questions.get(idx) {
            lines.push(Line::from(""));
            lines.push(Line::from(q.prompt.clone()));
            if !q.choices.is_empty() {
                lines.push(Line::from(Span::styled(
                    format!("choices: {}", q.choices.join(" | ")),
                    Style::default().add_modifier(Modifier::DIM),
                )));
            }
            if !q.required {
                lines.push(Line::from(Span::styled(
                    "(optional — Enter to skip)",
                    Style::default().add_modifier(Modifier::DIM),
                )));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "type your answer below, press Enter",
                Style::default().add_modifier(Modifier::DIM),
            )));
        }
        centered_box(f, area, 70, 11, " clarification needed ", lines);
        return;
    }
    if let Some(_approval_id) = state.pending.approve_approval_id.as_ref() {
        let mut lines = vec![Line::from(format!(
            "Plan revision {} awaits your decision",
            state.pending.approve_revision_number.unwrap_or(0)
        ))];
        if let Some(snap) = state.snapshot.as_ref() {
            for t in snap.tasks.iter().take(8) {
                lines.push(Line::from(format!("  {} {}", t.client_ref, t.title)));
            }
            if snap.tasks.len() > 8 {
                lines.push(Line::from(format!("  ... {} more", snap.tasks.len() - 8)));
            }
        }
        lines.push(Line::from(""));
        if state.pending.request_changes_mode {
            lines.push(Line::from(Span::styled(
                "editing mode — type change feedback below, Enter to send",
                Style::default().fg(Color::Yellow),
            )));
        } else {
            lines.push(Line::from("[a] approve   [e] request changes   [x] reject"));
        }
        centered_box(f, area, 70, 15, " plan approval ", lines);
        return;
    }
    if state.pending.help_open {
        let lines: Vec<Line> = super::commands::HELP_TEXT
            .lines()
            .map(|l| Line::from(l.to_string()))
            .collect();
        centered_box(f, area, 74, 22, " help (q to close) ", lines);
    }
}

fn centered_box(f: &mut Frame, area: Rect, pct_w: u16, min_h: u16, title: &str, lines: Vec<Line>) {
    let popup = centered_rect(
        pct_w,
        (min_h * 100 / area.height.max(1)).clamp(20, 90),
        area,
    );
    f.render_widget(Clear, popup);
    let p = Paragraph::new(lines).wrap(Wrap { trim: false }).block(
        Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(Style::default().fg(Color::Magenta)),
    );
    f.render_widget(p, popup);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
