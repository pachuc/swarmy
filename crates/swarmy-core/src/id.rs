use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

macro_rules! ulid_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Ulid);

        impl $name {
            /// Wrap an existing ULID; callers generate IDs so this crate stays free of clocks.
            #[must_use]
            pub const fn from_ulid(ulid: Ulid) -> Self {
                Self(ulid)
            }

            #[must_use]
            pub const fn as_ulid(self) -> Ulid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

ulid_id!(
    /// Identifies a durable agent: identity, home volume, mailbox, main session.
    AgentId
);
ulid_id!(
    /// Identifies one bounded context window and its event log.
    SessionId
);

/// Idempotency key for an inference request or tool call: `blake3(session_id, seq)`.
///
/// Deterministic from the session and step, so a retried step produces the same key and
/// the gateway can recognise a duplicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId([u8; 32]);

impl RequestId {
    #[must_use]
    pub fn for_step(session: SessionId, seq: u64) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&session.as_ulid().to_bytes());
        hasher.update(&seq.to_be_bytes());
        Self(*hasher.finalize().as_bytes())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_id_is_deterministic_per_step() {
        let session = SessionId::from_ulid(Ulid::from_parts(1, 2));
        assert_eq!(
            RequestId::for_step(session, 7),
            RequestId::for_step(session, 7)
        );
        assert_ne!(
            RequestId::for_step(session, 7),
            RequestId::for_step(session, 8)
        );
        let other = SessionId::from_ulid(Ulid::from_parts(1, 3));
        assert_ne!(
            RequestId::for_step(session, 7),
            RequestId::for_step(other, 7)
        );
    }

    #[test]
    fn ids_round_trip_through_serde() {
        let id = AgentId::from_ulid(Ulid::from_parts(5, 6));
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{id}\""));
        assert_eq!(serde_json::from_str::<AgentId>(&json).unwrap(), id);
    }
}
