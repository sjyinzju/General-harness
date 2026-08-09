//! Terminal lifecycle guard — raw mode + alternate screen, always restored.
//!
//! RAII plus a panic hook: no matter how the TUI exits (normal, error, or
//! panic) the user's terminal is left in a usable state.

use std::io::{Stdout, Write};
use std::panic::PanicHookInfo;
use std::sync::Arc;

use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

type PanicHook = Arc<dyn Fn(&PanicHookInfo<'_>) + Send + Sync + 'static>;

/// RAII guard: enters raw mode + alternate screen on construction and
/// restores the terminal on drop. Never panics during teardown.
pub struct TerminalGuard {
    previous_hook: Option<PanicHook>,
}

pub type TuiTerminal = Terminal<CrosstermBackend<Stdout>>;

impl TerminalGuard {
    /// Enter alternate screen + raw mode and install a panic hook that
    /// restores the terminal before the default panic output.
    pub fn enter() -> std::io::Result<(Self, TuiTerminal)> {
        let previous: PanicHook = std::panic::take_hook().into();
        let hook_prev = previous.clone();
        std::panic::set_hook(Box::new(move |info: &PanicHookInfo<'_>| {
            restore_terminal_quiet();
            hook_prev(info);
        }));

        enable_raw_mode()?;
        let mut stdout = std::io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(std::io::stdout());
        let terminal = Terminal::new(backend)?;

        Ok((
            Self {
                previous_hook: Some(previous),
            },
            terminal,
        ))
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal_quiet();
        if let Some(prev) = self.previous_hook.take() {
            std::panic::set_hook(Box::new(move |info: &PanicHookInfo<'_>| prev(info)));
        }
    }
}

/// Restore the terminal, swallowing errors (teardown must never panic).
pub fn restore_terminal_quiet() {
    let _ = disable_raw_mode();
    let _ = execute!(std::io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
    let _ = std::io::stdout().flush();
}

/// True when stdout looks like an interactive terminal. The no-arg
/// `general-harness` entry only opens the TUI for TTYs — scripts and CI
/// keep the traditional usage output (brief §compatibility).
pub fn stdout_is_tty() -> bool {
    use std::io::IsTerminal;
    std::io::stdout().is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_is_safe_to_call_without_entering() {
        // Must never panic even though we never entered raw mode.
        restore_terminal_quiet();
        restore_terminal_quiet();
    }

    #[test]
    fn tty_detection_returns_a_bool() {
        // Value depends on the test harness environment; just assert no panic.
        let _ = stdout_is_tty();
    }
}
