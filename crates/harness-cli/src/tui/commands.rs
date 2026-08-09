//! Slash-command parsing for the TUI input line.
//!
//! Commands are pure TUI UX; every mutation they trigger still travels
//! through IPC (pause/resume/cancel are Effects, never local state flips).

/// A recognized slash command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Help,
    Plan,
    Status,
    Usage,
    Pause,
    Resume,
    Cancel,
    Quit,
    Clear,
    Goals,
    /// Attach to a specific goal by id.
    Goal(String),
}

/// Parse a `/command` string; returns `None` for non-commands or unknowns.
pub fn parse_command(raw: &str) -> Option<Command> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('/') {
        return None;
    }
    let mut parts = trimmed.splitn(2, ' ');
    let head = parts.next()?.to_ascii_lowercase();
    let arg = parts.next().map(str::trim).unwrap_or("");
    match head.as_str() {
        "/help" | "/?" => Some(Command::Help),
        "/plan" => Some(Command::Plan),
        "/status" => Some(Command::Status),
        "/usage" => Some(Command::Usage),
        "/pause" => Some(Command::Pause),
        "/resume" => Some(Command::Resume),
        "/cancel" => Some(Command::Cancel),
        "/quit" | "/exit" => Some(Command::Quit),
        "/clear" => Some(Command::Clear),
        "/goals" => Some(Command::Goals),
        "/goal" if !arg.is_empty() => Some(Command::Goal(arg.to_string())),
        _ => None,
    }
}

/// Help text rendered by the /help overlay and summarized in the footer.
pub const HELP_TEXT: &str = "\
/help            show this help
/plan            show plan revision details in the conversation
/status          show goal status in the conversation
/usage           show usage summary in the conversation
/pause           pause the goal (IPC goal.pause)
/resume          resume the goal (IPC goal.resume)
/cancel          cancel the goal after confirmation (IPC goal.cancel)
/goals           list recent goals (read-only goal.list)
/goal <id>       attach to a goal by id
/clear           clear the conversation panel
/quit            exit the TUI — the goal KEEPS RUNNING in the Supervisor

Keys:
Enter            submit contextual input (goal / answer / changes / message)
Tab              cycle panel focus
Up/Down          scroll focused panel
PageUp/PageDown  scroll the conversation
Esc              close modal / cancel local edit
Ctrl+C           exit the TUI — does NOT cancel the goal";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_required_commands() {
        assert_eq!(parse_command("/help"), Some(Command::Help));
        assert_eq!(parse_command("/plan"), Some(Command::Plan));
        assert_eq!(parse_command("/status"), Some(Command::Status));
        assert_eq!(parse_command("/usage"), Some(Command::Usage));
        assert_eq!(parse_command("/pause"), Some(Command::Pause));
        assert_eq!(parse_command("/resume"), Some(Command::Resume));
        assert_eq!(parse_command("/cancel"), Some(Command::Cancel));
        assert_eq!(parse_command("/quit"), Some(Command::Quit));
        assert_eq!(parse_command("/clear"), Some(Command::Clear));
        assert_eq!(parse_command("/goals"), Some(Command::Goals));
        assert_eq!(
            parse_command("/goal g-123"),
            Some(Command::Goal("g-123".into()))
        );
    }

    #[test]
    fn non_commands_return_none() {
        assert_eq!(parse_command("hello"), None);
        assert_eq!(parse_command("rm -rf /"), None);
        assert_eq!(parse_command("/unknown"), None);
        assert_eq!(parse_command("/goal"), None);
        assert_eq!(parse_command(""), None);
    }

    #[test]
    fn case_and_whitespace_tolerant() {
        assert_eq!(parse_command("  /HELP  "), Some(Command::Help));
        assert_eq!(parse_command("/QUIT"), Some(Command::Quit));
        assert_eq!(
            parse_command("/goal  g-1 "),
            Some(Command::Goal("g-1".into()))
        );
    }
}
