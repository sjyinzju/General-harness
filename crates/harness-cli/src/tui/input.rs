//! Single-line input buffer editing — unicode-safe, no panics.
//!
//! The buffer stores `char`s (never byte slices), so CJK composition and
//! multi-byte characters cannot corrupt the cursor position.

/// A single-line editable input buffer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InputBuffer {
    chars: Vec<char>,
    /// Cursor position in characters (0..=len).
    pos: usize,
}

impl InputBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn text(&self) -> String {
        self.chars.iter().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.chars.is_empty()
    }

    /// Cursor position in characters.
    pub fn cursor(&self) -> usize {
        self.pos
    }

    /// Display width of the text before the cursor (unicode width is
    /// approximated: CJK-range chars count as 2 columns).
    pub fn display_cursor_col(&self) -> usize {
        self.chars
            .iter()
            .take(self.pos)
            .map(|c| char_width(*c))
            .sum()
    }

    pub fn insert(&mut self, c: char) {
        // Ignore control characters that reach us outside key handling.
        if c.is_control() {
            return;
        }
        self.chars.insert(self.pos, c);
        self.pos += 1;
    }

    pub fn backspace(&mut self) {
        if self.pos > 0 {
            self.pos -= 1;
            self.chars.remove(self.pos);
        }
    }

    pub fn delete(&mut self) {
        if self.pos < self.chars.len() {
            self.chars.remove(self.pos);
        }
    }

    pub fn move_left(&mut self) {
        self.pos = self.pos.saturating_sub(1);
    }

    pub fn move_right(&mut self) {
        if self.pos < self.chars.len() {
            self.pos += 1;
        }
    }

    pub fn move_home(&mut self) {
        self.pos = 0;
    }

    pub fn move_end(&mut self) {
        self.pos = self.chars.len();
    }

    /// Ctrl+U — clear the whole line.
    pub fn clear(&mut self) {
        self.chars.clear();
        self.pos = 0;
    }

    /// Take the current text, resetting the buffer.
    pub fn take(&mut self) -> String {
        let text = self.text();
        self.clear();
        text
    }
}

/// Approximate terminal display width: ASCII = 1, wide (CJK) ranges = 2.
pub fn char_width(c: char) -> usize {
    let cp = c as u32;
    if (0x1100..=0x115F).contains(&cp)
        || (0x2E80..=0xA4CF).contains(&cp)
        || (0xAC00..=0xD7A3).contains(&cp)
        || (0xF900..=0xFAFF).contains(&cp)
        || (0xFE30..=0xFE4F).contains(&cp)
        || (0xFF00..=0xFF60).contains(&cp)
        || (0xFFE0..=0xFFE6).contains(&cp)
        || (0x20000..=0x3FFFD).contains(&cp)
    {
        2
    } else {
        1
    }
}

/// Display width of a string (sum of per-char widths).
pub fn display_width(s: &str) -> usize {
    s.chars().map(char_width).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_backspace_ascii() {
        let mut b = InputBuffer::new();
        for c in "abc".chars() {
            b.insert(c);
        }
        assert_eq!(b.text(), "abc");
        b.backspace();
        assert_eq!(b.text(), "ab");
        b.move_home();
        b.delete();
        assert_eq!(b.text(), "b");
    }

    #[test]
    fn chinese_input_does_not_panic() {
        let mut b = InputBuffer::new();
        for c in "实现终端界面".chars() {
            b.insert(c);
        }
        assert_eq!(b.text(), "实现终端界面");
        // Cursor arithmetic stays in char space: cursor moves before 面,
        // backspace removes 界, then append at the end.
        b.move_left();
        b.backspace();
        assert_eq!(b.text(), "实现终端面");
        b.move_right();
        b.insert('！');
        assert_eq!(b.text(), "实现终端面！");
    }

    #[test]
    fn navigation_bounds_are_safe() {
        let mut b = InputBuffer::new();
        b.move_left();
        b.move_left();
        b.backspace();
        b.delete();
        b.move_right();
        assert_eq!(b.text(), "");
        b.insert('x');
        b.move_end();
        b.move_right();
        assert_eq!(b.cursor(), 1);
    }

    #[test]
    fn ctrl_u_clears_line() {
        let mut b = InputBuffer::new();
        for c in "hello".chars() {
            b.insert(c);
        }
        b.clear();
        assert!(b.is_empty());
        assert_eq!(b.cursor(), 0);
    }

    #[test]
    fn control_chars_are_ignored() {
        let mut b = InputBuffer::new();
        b.insert('\n');
        b.insert('\u{7f}');
        assert!(b.is_empty());
    }

    #[test]
    fn cjk_display_width_is_double() {
        assert_eq!(display_width("中"), 2);
        assert_eq!(display_width("a中"), 3);
    }
}
