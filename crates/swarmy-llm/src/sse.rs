//! Shared incremental server-sent-event framing for provider streams.

use crate::Error;

/// A complete SSE data payload or a non-SSE line (some providers return JSON errors).
pub enum Frame {
    Data(Vec<u8>),
    Raw(Vec<u8>),
}

/// Frames multiline data across arbitrary chunks, CRLF boundaries and comments.
#[derive(Default)]
pub struct SseParser {
    line: Vec<u8>,
    data: Vec<u8>,
    previous_cr: bool,
}

impl SseParser {
    /// # Errors
    /// Rejects an event exceeding 8 MiB.
    pub fn push_byte(&mut self, byte: u8) -> Result<Option<Frame>, Error> {
        if byte == b'\n' && self.previous_cr {
            self.previous_cr = false;
            return Ok(None);
        }
        self.previous_cr = byte == b'\r';
        if !matches!(byte, b'\r' | b'\n') {
            self.line.push(byte);
            if self.line.len() + self.data.len() > 8 * 1024 * 1024 {
                return Err(Error::Protocol("SSE event exceeds 8 MiB".into()));
            }
            return Ok(None);
        }
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            if self.data.is_empty() {
                return Ok(None);
            }
            return Ok(Some(Frame::Data(std::mem::take(&mut self.data))));
        }
        if let Some(data) = line.strip_prefix(b"data:") {
            self.data
                .extend_from_slice(data.strip_prefix(b" ").unwrap_or(data));
            self.data.push(b'\n');
            return Ok(None);
        }
        if line.starts_with(b":")
            || line.starts_with(b"event:")
            || line.starts_with(b"id:")
            || line.starts_with(b"retry:")
        {
            return Ok(None);
        }
        Ok(Some(Frame::Raw(line)))
    }

    /// Flush a final unterminated event at EOF (used by Gemini).
    /// # Errors
    /// Rejects an oversized trailing line.
    pub fn finish(&mut self) -> Result<Option<Frame>, Error> {
        if !self.line.is_empty() {
            let _ = self.push_byte(b'\n')?;
        }
        self.push_byte(b'\n')
    }

    #[must_use]
    pub fn pending_line(&self) -> &[u8] {
        &self.line
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_fixtures_have_stable_frames_at_every_chunk_size() {
        for (fixture, count) in [
            (include_str!("../tests/fixtures/text.sse"), 5),
            (include_str!("../tests/fixtures/function.sse"), 5),
            (include_str!("../tests/fixtures/reasoning.sse"), 3),
            (include_str!("../tests/fixtures/error.sse"), 1),
        ] {
            for width in [1, 2, 7, 31, fixture.len()] {
                let mut parser = SseParser::default();
                let mut events = Vec::new();
                for chunk in fixture.as_bytes().chunks(width) {
                    for &byte in chunk {
                        if let Some(Frame::Data(data)) = parser.push_byte(byte).unwrap() {
                            events
                                .push(serde_json::from_slice::<serde_json::Value>(&data).unwrap());
                        }
                    }
                }
                assert_eq!(events.len(), count);
            }
        }
    }

    #[test]
    fn frames_split_utf8_crlf_multiline_and_comments() {
        let mut parser = SseParser::default();
        let mut frames = Vec::new();
        for chunk in [
            b": comment\r".as_slice(),
            b"\ndata: \xc3",
            b"\xa9\r\ndata: next\n\n",
        ] {
            for &byte in chunk {
                if let Some(frame) = parser.push_byte(byte).unwrap() {
                    frames.push(frame);
                }
            }
        }
        assert!(matches!(&frames[..], [Frame::Data(data)] if data == "é\nnext\n".as_bytes()));
    }
}
