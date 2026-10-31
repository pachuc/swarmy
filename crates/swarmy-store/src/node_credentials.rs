//! Hashed credentials for node API authentication.
use crate::{Result, Store, StoreError, read, write};
use jiff::Timestamp;
use rand::TryRngCore;
use swarmy_core::{NodeId, NodeToken};

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredNodeCredential {
    hash: [u8; 32],
    issued_at: Timestamp,
}

impl Store {
    /// Issue a fresh credential, replacing any prior credential.
    /// # Errors
    /// Randomness and storage failures.
    pub async fn issue_node_credential(&self, node: NodeId) -> Result<NodeToken> {
        let mut secret = [0; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut secret)
            .map_err(|error| StoreError::Storage(crate::StorageError::Randomness(error.into())))?;
        let token = NodeToken::new(node, secret);
        let row = StoredNodeCredential {
            hash: token.secret_hash(),
            issued_at: self.now(),
        };
        self.transaction(|trx| {
            let row = &row;
            async move { write(&trx, &self.keys().node_credential(node), row) }
        })
        .await?;
        Ok(token)
    }
    /// Check the current credential using constant-time hash equality.
    /// # Errors
    /// Storage and decoding failures.
    pub async fn verify_node_credential(&self, token: &NodeToken) -> Result<bool> {
        let row = self
            .transaction(|trx| async move {
                read::<StoredNodeCredential>(&trx, &self.keys().node_credential(token.node())).await
            })
            .await?;
        Ok(row.is_some_and(|row| {
            blake3::Hash::from_bytes(row.hash) == blake3::Hash::from_bytes(token.secret_hash())
        }))
    }
    /// Revoke the credential, returning whether one existed.
    /// # Errors
    /// Storage failures.
    pub async fn revoke_node_credential(&self, node: NodeId) -> Result<bool> {
        self.transaction(|trx| async move {
            let key = self.keys().node_credential(node);
            let existed = read::<StoredNodeCredential>(&trx, &key).await?.is_some();
            if existed {
                trx.clear(&key);
            }
            Ok(existed)
        })
        .await
    }
}
