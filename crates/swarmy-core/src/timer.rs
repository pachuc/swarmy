use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::{AgentId, SessionId, TimerId};

pub const MAX_AGENT_TIMERS: usize = 32;
pub const MAX_TIMER_NOTE_BYTES: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimerStatus {
    Pending,
    Fired { session_id: SessionId, seq: u64 },
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerRecord {
    pub timer_id: TimerId,
    pub agent_id: AgentId,
    pub due_at: Timestamp,
    pub note: String,
    pub status: TimerStatus,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetTimerArguments {
    pub delay_seconds: Option<u64>,
    pub at: Option<Timestamp>,
    pub note: String,
}

impl SetTimerArguments {
    /// Resolve exactly one time specification and bound the stored note.
    /// # Errors
    /// Rejects empty or oversized notes, ambiguous times, zero delays, and overflow.
    pub fn due_at(&self, now: Timestamp) -> Result<Timestamp, String> {
        if self.note.trim().is_empty() || self.note.len() > MAX_TIMER_NOTE_BYTES {
            return Err("note must contain 1-1024 UTF-8 bytes and not be blank".into());
        }
        match (self.delay_seconds, self.at) {
            (Some(seconds), None) if seconds > 0 => now
                .checked_add(std::time::Duration::from_secs(seconds))
                .map_err(|error| error.to_string()),
            (None, Some(at)) => Ok(at),
            _ => Err("provide exactly one of positive delay_seconds or at (RFC 3339)".into()),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelTimerArguments {
    pub timer_id: TimerId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validates_times_notes_and_unknown_fields() {
        let now: Timestamp = "2026-09-18T00:00:00Z".parse().unwrap();
        for value in [
            json!({"delay_seconds": 120, "note": "hello"}),
            json!({"at": "2026-09-18T00:02:00Z", "note": "hello"}),
        ] {
            let args: SetTimerArguments = serde_json::from_value(value).unwrap();
            assert_eq!(
                args.due_at(now).unwrap(),
                now.checked_add(std::time::Duration::from_secs(120))
                    .unwrap()
            );
        }
        for value in [
            json!({"note":"hello"}),
            json!({"delay_seconds":0,"note":"hello"}),
            json!({"delay_seconds":u64::MAX,"note":"hello"}),
            json!({"delay_seconds":1,"at":now,"note":"hello"}),
            json!({"delay_seconds":1,"note":" "}),
            json!({"delay_seconds":1,"note":"x".repeat(1025)}),
        ] {
            assert!(
                serde_json::from_value::<SetTimerArguments>(value)
                    .unwrap()
                    .due_at(now)
                    .is_err()
            );
        }
        assert!(
            serde_json::from_value::<SetTimerArguments>(
                json!({"delay_seconds":1,"note":"x","typo":1})
            )
            .is_err()
        );
    }
}
