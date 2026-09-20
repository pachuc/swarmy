//! Prefer the cluster record; only absent records fall back to the legacy file.
use std::{sync::OnceLock, time::Duration};

use futures::future::BoxFuture;
use swarmy_config::Keyring;
use swarmy_core::CredentialScope;
use swarmy_llm::{
    Error,
    auth::{CredentialLock, CredentialStore, Credentials, FileCredentialStore, OAuthClient},
};
use swarmy_store::{Store, StoreError};

pub struct ClusterCredentials {
    store: Store,
    keyring: Option<Keyring>,
    file: FileCredentialStore,
    account: OnceLock<String>,
}

impl ClusterCredentials {
    /// Open cluster credentials with the legacy file as an absent-record fallback.
    /// # Errors
    /// Returns unavailable keys or invalid stored credentials.
    pub async fn new(store: Store, path: &str) -> crate::Result<Self> {
        let keyring = match Keyring::load() {
            Ok(key) => Some(key),
            Err(swarmy_config::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                if store
                    .has_credential(CredentialScope::Cluster, "chatgpt")
                    .await?
                {
                    return Err(crate::Error::Configuration(
                        "cluster ChatGPT credential exists but keyring is missing; copy the cluster keyring or set SWARMY_KEYRING",
                    ));
                }
                tracing::warn!(
                    "keyring missing; using file-based ChatGPT credentials until a cluster keyring is installed"
                );
                None
            }
            Err(e) => return Err(e.into()),
        };
        let result = Self {
            store,
            keyring,
            file: FileCredentialStore::new(path),
            account: OnceLock::new(),
        };
        // Fail at startup for a wrong key instead of silently using stale file tokens.
        if result
            .store
            .has_credential(CredentialScope::Cluster, "chatgpt")
            .await?
        {
            result.load().await?;
        }
        Ok(result)
    }

    async fn stored(&self) -> Result<Option<swarmy_core::CredentialRecord>, Error> {
        if let Some(key) = &self.keyring {
            self.store
                .credentials(key.clone())
                .get_credential(CredentialScope::Cluster, "chatgpt")
                .await
                .map_err(|error| store_error(&error))
        } else if self
            .store
            .has_credential(CredentialScope::Cluster, "chatgpt")
            .await
            .map_err(|error| store_error(&error))?
        {
            Err(Error::Credentials(
                "cluster credential requires a keyring; restart after installing it",
            ))
        } else {
            Ok(None)
        }
    }

    fn check(&self, credentials: Credentials) -> Result<Credentials, Error> {
        if self.account.get_or_init(|| credentials.account_id().into()) != credentials.account_id()
        {
            return Err(Error::AccountChanged);
        }
        Ok(credentials)
    }
}

fn store_error(error: &StoreError) -> Error {
    tracing::warn!(%error, "cluster credential operation failed");
    Error::Credentials(
        "cluster credential unavailable; check keyring or run swarmy auth check chatgpt",
    )
}

impl CredentialStore for ClusterCredentials {
    fn load(&self) -> BoxFuture<'_, Result<Credentials, Error>> {
        Box::pin(async {
            let credentials = match self.stored().await? {
                Some(record) => Credentials::from_record(&record)?,
                None => self.file.load().await?,
            };
            self.check(credentials)
        })
    }

    fn save(&self, credentials: Credentials) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            if self.stored().await?.is_some() {
                return Err(Error::Credentials(
                    "cluster credentials require a fenced refresh",
                ));
            }
            self.file.save(self.check(credentials)?).await
        })
    }

    fn lock_refresh<'a>(
        &'a self,
        account_id: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialLock>, Error>> {
        Box::pin(async move {
            if self.stored().await?.is_some() {
                return Err(Error::Credentials(
                    "cluster credentials require a fenced refresh",
                ));
            }
            self.file.lock_refresh(account_id).await
        })
    }

    fn refresh<'a>(
        &'a self,
        client: &'a OAuthClient,
        observed: &'a Credentials,
    ) -> BoxFuture<'a, Result<Credentials, Error>> {
        Box::pin(async move {
            let Some(record) = self.stored().await? else {
                return client.refresh(&self.file, observed).await;
            };
            let current = self.check(Credentials::from_record(&record)?)?;
            if current.account_id() != observed.account_id() {
                return Err(Error::AccountChanged);
            }
            if current != *observed {
                return Ok(current);
            }
            let key = self
                .keyring
                .clone()
                .ok_or(Error::Credentials("missing cluster keyring"))?;
            let updated = self
                .store
                .credentials(key)
                .refresh_with_lease(
                    CredentialScope::Cluster,
                    "chatgpt",
                    Duration::from_secs(45),
                    |record| async move {
                        let current = Credentials::from_record(&record)
                            .map_err(|_| StoreError::CredentialRefresh)?;
                        if current != *observed {
                            return Ok(record);
                        }
                        client
                            .refresh_credentials(current)
                            .await
                            .and_then(|c| c.to_record())
                            .map_err(|_| StoreError::CredentialRefresh)
                    },
                )
                .await
                .map_err(|error| store_error(&error))?;
            self.check(Credentials::from_record(&updated)?)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use foundationdb::{Database, tuple::Subspace};
    use std::sync::Arc;
    use swarmy_store::blob::MemoryBlobStore;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    #[tokio::test]
    async fn cluster_wins_and_refresh_is_shared_between_gateways() {
        static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            eprintln!("skipping gateway credential integration: SWARMY_FDB_CLUSTER_FILE unset");
            return;
        };
        NETWORK.get_or_init(swarmy_store::boot);
        let store = Store::with_subspace(
            Arc::new(Database::new(Some(&cluster)).unwrap()),
            Subspace::all().subspace(&("gateway-auth-tests", ulid::Ulid::generate().to_string())),
            Arc::new(MemoryBlobStore::default()),
        );
        let files = tempfile::tempdir().unwrap();
        let auth_path = files.path().join("auth.json");
        let fixture = include_str!("../../swarmy-llm/tests/fixtures/auth.json");
        std::fs::write(&auth_path, fixture).unwrap();
        let make = || ClusterCredentials {
            store: store.clone(),
            keyring: Some(Keyring::from_bytes([3; 32])),
            file: FileCredentialStore::new(&auth_path),
            account: OnceLock::new(),
        };
        let first = make();
        let second = make();
        let file_credentials = first.load().await.unwrap();
        let credentials = store.credentials(Keyring::from_bytes([3; 32]));
        credentials
            .put_credential(
                CredentialScope::Cluster,
                "chatgpt",
                &file_credentials.to_record().unwrap(),
            )
            .await
            .unwrap();
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/oauth/token")).respond_with(
            ResponseTemplate::new(200).set_delay(Duration::from_millis(150)).set_body_json(serde_json::json!({"access_token": "new-access", "refresh_token": "new-refresh"}))
        ).expect(1).mount(&server).await;
        let client = OAuthClient::with_issuer(&server.uri()).unwrap();
        let (a, b) = tokio::join!(
            first.refresh(&client, &file_credentials),
            second.refresh(&client, &file_credentials)
        );
        assert_eq!(a.unwrap().access_token(), "new-access");
        assert_eq!(b.unwrap().access_token(), "new-access");
        assert_eq!(second.load().await.unwrap().access_token(), "new-access");
        assert_eq!(std::fs::read_to_string(&auth_path).unwrap(), fixture);
        let wrong = ClusterCredentials {
            keyring: Some(Keyring::from_bytes([4; 32])),
            ..make()
        };
        assert!(
            wrong.load().await.is_err(),
            "wrong key must not fall back to file"
        );
        let missing = ClusterCredentials {
            keyring: None,
            ..make()
        };
        assert!(missing.load().await.is_err());
        credentials
            .delete_credential(CredentialScope::Cluster, "chatgpt")
            .await
            .unwrap();
        assert!(first.load().await.unwrap() == file_credentials);
    }
}
