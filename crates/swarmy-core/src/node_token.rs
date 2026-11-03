//! Node API bearer token: `swn.<node ulid>.<base64url secret>`.
use crate::NodeId;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use std::fmt;

const PREFIX: &str = "swn";

/// A node credential. Only its secret hash is stored.
#[derive(Clone, PartialEq, Eq)]
pub struct NodeToken {
    node: NodeId,
    secret: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum NodeTokenError {
    #[error("node token must look like swn.<node id>.<secret>")]
    Malformed,
}

impl NodeToken {
    #[must_use]
    pub const fn new(node: NodeId, secret: [u8; 32]) -> Self {
        Self { node, secret }
    }
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }
    #[must_use]
    pub fn secret_hash(&self) -> [u8; 32] {
        *blake3::hash(&self.secret).as_bytes()
    }
    /// Deliberately reveal the bearer text.
    #[must_use]
    pub fn reveal(&self) -> String {
        format!(
            "{PREFIX}.{}.{}",
            self.node,
            URL_SAFE_NO_PAD.encode(self.secret)
        )
    }
    /// Parse a token, ignoring surrounding whitespace.
    /// # Errors
    /// Rejects malformed tokens without exposing their contents.
    pub fn parse(text: &str) -> Result<Self, NodeTokenError> {
        let mut parts = text.trim().splitn(4, '.');
        let (Some("swn"), Some(node), Some(secret), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(NodeTokenError::Malformed);
        };
        let node = ulid::Ulid::from_string(node).map_err(|_| NodeTokenError::Malformed)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(secret)
            .map_err(|_| NodeTokenError::Malformed)?;
        let secret = bytes.try_into().map_err(|_| NodeTokenError::Malformed)?;
        Ok(Self::new(NodeId::from_ulid(node), secret))
    }
}
impl fmt::Debug for NodeToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeToken")
            .field("node", &self.node)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn round_trip_and_malformed() {
        let node = NodeId::from_ulid(ulid::Ulid::from(1_u128));
        let token = NodeToken::new(node, [7; 32]);
        assert_eq!(NodeToken::parse(&token.reveal()).unwrap(), token);
        assert_eq!(
            NodeToken::parse(&format!("{}\n", token.reveal())).unwrap(),
            token
        );
        for bad in [
            "bad.node.secret",
            "swn.node",
            "swn.node.secret.extra",
            "swn.bad.aaaa",
        ] {
            assert!(matches!(
                NodeToken::parse(bad),
                Err(NodeTokenError::Malformed)
            ));
        }
        for length in [31, 33] {
            let text = format!("swn.{node}.{}", URL_SAFE_NO_PAD.encode(vec![7; length]));
            assert!(matches!(
                NodeToken::parse(&text),
                Err(NodeTokenError::Malformed)
            ));
        }
        assert!(matches!(
            NodeToken::parse(&format!("{}=", token.reveal())),
            Err(NodeTokenError::Malformed)
        ));
        assert!(!format!("{token:?}").contains(token.reveal().split('.').next_back().unwrap()));
        assert_ne!(
            token.secret_hash(),
            NodeToken::new(node, [8; 32]).secret_hash()
        );
    }
}
