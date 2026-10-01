//! Versioned session record: the stored format and its working copy.
use serde::{Deserialize, Serialize};
use swarmy_core::{SessionId, SessionState};

use crate::INLINE_LIMIT;

pub(crate) const SESSION_RECORD_VERSION: u8 = 2;
// Postcard encodes a session id with a 26-byte prefix, so this marker cannot
// collide with an inline session record. Oversized records need bounded chunks.
pub(crate) const SESSION_CHUNK_MARKER: u8 = 0xff;
pub(crate) const SESSION_MAX_BYTES: usize = 10 * INLINE_LIMIT;

/// The current format owns all session-local metadata. Postcard fields are
/// positional; changes require a new fixed-byte fixture and a one-way break.
#[derive(Serialize, Deserialize)]
pub(crate) struct StoredSession {
    pub(crate) session_id: SessionId,
    pub(crate) agent_id: swarmy_core::AgentId,
    pub(crate) state: SessionState,
    pub(crate) head_seq: u64,
    pub(crate) snapshot_seq: Option<u64>,
    pub(crate) kind: swarmy_core::SessionKind,
    pub(crate) computer_deleted: bool,
    pub(crate) plan: Vec<swarmy_core::PlanStep>,
    pub(crate) inference: swarmy_core::InferenceSelection,
    pub(crate) interrupt_requested: bool,
    pub(crate) route: Option<String>,
    pub(crate) route_step: u32,
    pub(crate) image: Option<swarmy_core::ImageRecord>,
    pub(crate) idle_since: Option<jiff::Timestamp>,
    pub(crate) state_since: Option<jiff::Timestamp>,
}

#[cfg(test)]
mod stored_format_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

    #[test]
    fn fixed_versioned_session_bytes() {
        let id = SessionId::from_ulid(ulid::Ulid::from(0_u128));
        let agent = swarmy_core::AgentId::from_ulid(ulid::Ulid::from(0_u128));
        let v2 = StoredSession {
            session_id: id,
            agent_id: agent,
            state: SessionState::Idle,
            head_seq: 0,
            snapshot_seq: None,
            kind: swarmy_core::SessionKind::Ephemeral,
            computer_deleted: false,
            plan: Vec::new(),
            inference: swarmy_core::InferenceSelection::default(),
            interrupt_requested: false,
            route: None,
            route_step: 0,
            image: None,
            idle_since: None,
            state_since: None,
        };
        let mut bytes = vec![SESSION_RECORD_VERSION];
        bytes.extend(postcard::to_allocvec(&v2).unwrap());
        let mut v2_bytes = vec![SESSION_RECORD_VERSION, 26];
        v2_bytes.extend([b'0'; 26]);
        v2_bytes.push(26);
        v2_bytes.extend([b'0'; 26]);
        v2_bytes.extend([0, 0, 0]);
        v2_bytes.extend([0; 12]); // Kind through state-since are empty defaults.
        assert_eq!(bytes, v2_bytes);
        let decoded: StoredSession = postcard::from_bytes(&bytes[1..]).unwrap();
        assert_eq!(decoded.session_id, id);
        assert_eq!(decoded.route_step, 0);
    }

    #[test]
    fn fixed_nondefault_v2_session_bytes() {
        let id = SessionId::from_ulid(ulid::Ulid::from(0_u128));
        let v2 = StoredSession {
            session_id: id,
            agent_id: swarmy_core::AgentId::from_ulid(ulid::Ulid::from(0_u128)),
            state: SessionState::Runnable,
            head_seq: 0,
            snapshot_seq: None,
            kind: swarmy_core::SessionKind::Ephemeral,
            computer_deleted: false,
            plan: Vec::new(),
            inference: swarmy_core::InferenceSelection::default(),
            interrupt_requested: true,
            route: None,
            route_step: 3,
            image: None,
            idle_since: None,
            state_since: None,
        };
        let mut expected = vec![SESSION_RECORD_VERSION, 26];
        expected.extend([b'0'; 26]);
        expected.push(26);
        expected.extend([b'0'; 26]);
        expected.extend([1, 0, 0]); // Runnable, empty log and snapshot.
        expected.extend([0, 0, 0, 0, 0, 0, 1, 0, 3, 0, 0, 0]);
        let mut actual = vec![SESSION_RECORD_VERSION];
        actual.extend(postcard::to_allocvec(&v2).unwrap());
        assert_eq!(actual, expected);
        let decoded: StoredSession = postcard::from_bytes(&expected[1..]).unwrap();
        assert!(decoded.interrupt_requested);
        assert_eq!(decoded.route_step, 3);
    }
}
