use unicode_width::UnicodeWidthStr;

#[derive(Default)]
pub struct Input {
    pub text: String,
    cursor: usize,
}

impl Input {
    pub fn insert(&mut self, c: char) {
        if !c.is_control() {
            self.text.insert(self.cursor, c);
            self.cursor += c.len_utf8();
        }
    }

    pub fn left(&mut self) {
        self.cursor = self.text[..self.cursor]
            .char_indices()
            .next_back()
            .map_or(0, |(index, _)| index);
    }

    pub fn right(&mut self) {
        if let Some(c) = self.text[self.cursor..].chars().next() {
            self.cursor += c.len_utf8();
        }
    }

    pub fn backspace(&mut self) {
        let end = self.cursor;
        self.left();
        self.text.drain(self.cursor..end);
    }

    pub fn take(&mut self) -> String {
        self.cursor = 0;
        std::mem::take(&mut self.text)
    }

    pub fn view(&self, width: u16) -> (String, u16) {
        let available = usize::from(width.saturating_sub(3));
        let mut start = 0;
        while self.text[start..self.cursor].width() > available {
            start += self.text[start..].chars().next().map_or(0, char::len_utf8);
        }
        let cursor = u16::try_from(self.text[start..self.cursor].width()).unwrap_or(u16::MAX);
        (
            format!("> {}", &self.text[start..]),
            cursor.saturating_add(2).min(width.saturating_sub(1)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edits_unicode_at_cursor_and_keeps_cursor_visible() {
        let mut input = Input::default();
        for c in "a界éz".chars() {
            input.insert(c);
        }
        input.left();
        input.backspace();
        input.insert('🙂');
        assert_eq!(input.text, "a界🙂z");
        assert_eq!(input.view(5), ("> 🙂z".into(), 4));
        input.right();
        assert_eq!(input.take(), "a界🙂z");
        input.backspace();
        assert_eq!(input.view(0).1, 0);
    }
}
