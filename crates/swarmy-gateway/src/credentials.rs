//! Cluster persistence for the provider-neutral credential resolver.
use async_trait::async_trait;
use std::time::Duration;
use swarmy_config::Keyring;
use swarmy_core::{CredentialRecord, CredentialScope, CredentialStatus};
use swarmy_llm::{
    Error,
    auth::{AuthStore, Login},
};
use swarmy_store::{Store, StoreError};

pub struct ClusterCredentials {
    store: Store,
    credentials: Option<swarmy_store::credentials::CredentialStore>,
}

impl ClusterCredentials {
    /// Open the cluster credential store. Without a keyring, stored records are
    /// refused and only environment credentials remain available.
    /// # Errors
    /// Returns an unreadable keyring, a wrong key, or an unavailable database.
    pub async fn new(store: Store) -> crate::Result<Self> {
        let credentials = match Keyring::load() {
            Ok(keyring) => Some(store.credentials(keyring)),
            Err(swarmy_config::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!(
                    "keyring missing; stored credentials are unavailable until a cluster keyring is installed"
                );
                None
            }
            Err(e) => return Err(e.into()),
        };
        if let Some(credentials) = &credentials {
            // Diagnose wrong keys before accepting requests for any stored provider.
            credentials.list_entries(CredentialScope::Cluster).await?;
        }
        Ok(Self { store, credentials })
    }

    #[cfg(test)]
    fn with_credentials(
        store: &Store,
        credentials: swarmy_store::credentials::CredentialStore,
    ) -> Self {
        Self {
            store: store.clone(),
            credentials: Some(credentials),
        }
    }
}

fn store_error(error: &StoreError) -> Error {
    tracing::warn!(%error, "cluster credential operation failed");
    Error::Credentials("cluster credential unavailable; check keyring and database")
}

#[async_trait]
impl AuthStore for ClusterCredentials {
    async fn get(&self, provider: &str) -> Result<Option<CredentialRecord>, Error> {
        Ok(self.get_labelled(provider).await?.map(|(_, record)| record))
    }

    async fn get_labelled(
        &self,
        provider: &str,
    ) -> Result<Option<(Option<String>, CredentialRecord)>, Error> {
        let Some(credentials) = &self.credentials else {
            return if self
                .store
                .has_credential(CredentialScope::Cluster, provider)
                .await
                .map_err(|error| store_error(&error))?
            {
                Err(Error::Credentials(
                    "stored credential requires a readable cluster keyring",
                ))
            } else {
                Ok(None)
            };
        };
        let entries = credentials
            .provider_entries(CredentialScope::Cluster, provider)
            .await
            .map_err(|error| store_error(&error))?;
        if entries.is_empty() {
            return Ok(None);
        }
        let now = jiff::Timestamp::now();
        let mut pool: Vec<&(String, CredentialRecord)> = entries
            .iter()
            .filter(|(_, record)| record.status(now) == CredentialStatus::Ready)
            .collect();
        if pool.is_empty() {
            pool = entries.iter().collect();
        }
        // Serve the first entry whose breaker is closed so a rate limit on one
        // key leaves the provider's other entries usable. When every candidate
        // is open, return the earliest retry so the caller parks behind it.
        let mut earliest: Option<(jiff::Timestamp, usize)> = None;
        for (index, (label, _)) in pool.iter().enumerate() {
            let key = swarmy_store::CredentialKey::entry(provider, label);
            match self
                .store
                .entry_open_until(&key, now)
                .await
                .map_err(|error| store_error(&error))?
            {
                None => {
                    let (label, record) = &pool[index];
                    return Ok(Some((Some(label.clone()), record.clone())));
                }
                Some(until) => {
                    if earliest.is_none_or(|(at, _)| until < at) {
                        earliest = Some((until, index));
                    }
                }
            }
        }
        let (_, index) = earliest.unwrap_or((now, 0));
        let (label, record) = &pool[index];
        Ok(Some((Some(label.clone()), record.clone())))
    }

    async fn get_exact(
        &self,
        provider: &str,
        label: &str,
    ) -> Result<Option<CredentialRecord>, Error> {
        let Some(credentials) = &self.credentials else {
            return if self
                .store
                .has_credential(CredentialScope::Cluster, provider)
                .await
                .map_err(|error| store_error(&error))?
            {
                Err(Error::Credentials(
                    "stored credential requires a readable cluster keyring",
                ))
            } else {
                Ok(None)
            };
        };
        credentials
            .get_entry(CredentialScope::Cluster, provider, label)
            .await
            .map_err(|error| store_error(&error))
    }

    async fn refresh(
        &self,
        provider: &str,
        observed: &CredentialRecord,
        login: &dyn Login,
    ) -> Result<CredentialRecord, Error> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(Error::Credentials("missing cluster keyring"))?;
        // Read only this provider's entries in one transaction. When another
        // gateway already rotated the entry, the observed record matches no
        // current entry; fall back to the first entry so the fenced refresh
        // below adopts the winner's record instead of failing the turn.
        let entries = credentials
            .provider_entries(CredentialScope::Cluster, provider)
            .await
            .map_err(|error| store_error(&error))?;
        let mut selected = None;
        for (label, record) in &entries {
            if record == observed {
                selected = Some(label.clone());
                break;
            }
        }
        let label = match selected {
            Some(label) => label,
            None => credentials
                .first_entry(CredentialScope::Cluster, provider)
                .await
                .map_err(|error| store_error(&error))?
                .map(|(label, _)| label)
                .ok_or(Error::NeedsLogin(provider.into()))?,
        };
        credentials
            .refresh_entry_with_lease(
                CredentialScope::Cluster,
                provider,
                &label,
                Duration::from_secs(45),
                |current| async move {
                    if current != *observed {
                        return Ok(current);
                    }
                    let kind = login
                        .refresh(&current.kind)
                        .await
                        .map_err(|_| StoreError::CredentialRefresh)?
                        .ok_or(StoreError::CredentialRefresh)?;
                    Ok(CredentialRecord {
                        kind,
                        updated_at: jiff::Timestamp::now(),
                    })
                },
            )
            .await
            .map_err(|_| Error::NeedsLogin(provider.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use foundationdb::{Database, tuple::Subspace};
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, OnceLock,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use swarmy_core::{CredentialKind, CredentialStatus};
    use swarmy_llm::{
        ClientAuth,
        auth::{Credentials, LoginUi, OAuthClient, Resolver},
    };
    use swarmy_store::blob::MemoryBlobStore;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    fn test_store() -> Option<Store> {
        static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            eprintln!("skipping gateway auth integration: SWARMY_FDB_CLUSTER_FILE unset");
            return None;
        };
        NETWORK.get_or_init(swarmy_store::boot);
        Some(Store::with_subspace(
            Arc::new(Database::new(Some(&cluster)).unwrap()),
            Subspace::all().subspace(&("gateway-login-tests", ulid::Ulid::generate().to_string())),
            Arc::new(MemoryBlobStore::default()),
        ))
    }

    fn imported() -> CredentialRecord {
        let mut record = Credentials::from_json(
            serde_json::from_str(include_str!("../../swarmy-llm/tests/fixtures/auth.json"))
                .unwrap(),
        )
        .unwrap()
        .to_record()
        .unwrap();
        let CredentialKind::OAuth { expires_at, .. } = &mut record.kind else {
            unreachable!()
        };
        *expires_at = jiff::Timestamp::now()
            .checked_add(Duration::from_secs(120))
            .unwrap();
        record
    }

    #[tokio::test]
    async fn racing_resolvers_refresh_once_and_provider_reloads_imported_store() {
        use futures::TryStreamExt;
        use swarmy_llm::{GenerationSettings, Provider, Request};
        let Some(store) = test_store() else {
            return;
        };
        let credentials = store.credentials(Keyring::from_bytes([3; 32]));
        credentials
            .put_entry(CredentialScope::Cluster, "chatgpt", "default", &imported())
            .await
            .unwrap();
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/oauth/token")).respond_with(
            ResponseTemplate::new(200).set_delay(Duration::from_millis(150)).set_body_json(serde_json::json!({"access_token":"new-access", "refresh_token":"new-refresh"}))
        ).expect(1).mount(&server).await;
        let oauth = OAuthClient::with_issuer(&server.uri()).unwrap();
        let make = || {
            Resolver::with_chatgpt(
                Arc::new(ClusterCredentials::with_credentials(
                    &store,
                    credentials.clone(),
                )),
                oauth.clone(),
            )
        };
        let first = make();
        let second = make();
        let (a, b) = tokio::join!(first.resolve("chatgpt"), second.resolve("chatgpt"));
        for auth in [a.unwrap(), b.unwrap()] {
            let ClientAuth::ChatGpt(store) = auth.auth else {
                panic!("expected ChatGPT");
            };
            assert_eq!(store.load().await.unwrap().access_token(), "new-access");
        }
        let ClientAuth::ChatGpt(provider_store) = first.resolve("chatgpt").await.unwrap().auth
        else {
            unreachable!()
        };
        let provider = swarmy_llm::chatgpt::ChatGptProvider::with_endpoints(
            provider_store,
            &server.uri(),
            oauth,
        )
        .unwrap();
        for access in ["new-access", "externally-replaced"] {
            Mock::given(path("/responses"))
                .and(wiremock::matchers::header(
                    "authorization",
                    format!("Bearer {access}"),
                ))
                .respond_with(ResponseTemplate::new(200).set_body_raw(
                    include_str!("../../swarmy-llm/tests/fixtures/text.sse"),
                    "text/event-stream",
                ))
                .expect(1)
                .mount(&server)
                .await;
        }
        let request = || Request {
            system_prompt: String::new(),
            messages: vec![],
            tools: vec![],
            settings: GenerationSettings::default(),
        };
        provider
            .request(request())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut updated = credentials
            .get_entry(CredentialScope::Cluster, "chatgpt", "default")
            .await
            .unwrap()
            .unwrap();
        let CredentialKind::OAuth { access, .. } = &mut updated.kind else {
            unreachable!()
        };
        *access = "externally-replaced".into();
        credentials
            .put_entry(CredentialScope::Cluster, "chatgpt", "default", &updated)
            .await
            .unwrap();
        provider
            .request(request())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        credentials
            .delete_entry(CredentialScope::Cluster, "chatgpt", "default")
            .await
            .unwrap();
        assert!(
            provider
                .request(request())
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn failed_refresh_marks_record_and_does_not_retry() {
        let Some(store) = test_store() else {
            return;
        };
        let credentials = store.credentials(Keyring::from_bytes([5; 32]));
        credentials
            .put_entry(CredentialScope::Cluster, "chatgpt", "default", &imported())
            .await
            .unwrap();
        let server = MockServer::start().await;
        Mock::given(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;
        let resolver = Resolver::with_chatgpt(
            Arc::new(ClusterCredentials::with_credentials(
                &store,
                credentials.clone(),
            )),
            OAuthClient::with_issuer(&server.uri()).unwrap(),
        );
        for _ in 0..2 {
            assert!(matches!(
                resolver.resolve("chatgpt").await,
                Err(Error::NeedsLogin(_))
            ));
        }
        let record = credentials
            .get_entry(CredentialScope::Cluster, "chatgpt", "default")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.status(jiff::Timestamp::now()),
            CredentialStatus::NeedsLogin
        );
        assert!(
            matches!(record.kind, CredentialKind::OAuth { access, .. } if access == "access-fixture")
        );
    }
    #[tokio::test]
    async fn openrouter_login_key_is_persisted_and_wrong_key_never_falls_back() {
        use swarmy_llm::auth::OpenRouterLogin;
        struct Ui;
        #[async_trait]
        impl LoginUi for Ui {
            async fn notify_url(&self, _: &str) -> Result<(), Error> {
                Ok(())
            }
            async fn notify_device_code(&self, _: &str, _: &str) -> Result<(), Error> {
                unreachable!()
            }
            async fn prompt_secret(&self, _: &str) -> Result<String, Error> {
                Ok("approved".into())
            }
            async fn prompt_choice(&self, _: &str, _: &[&str]) -> Result<usize, Error> {
                Ok(1)
            }
        }
        let Some(store) = test_store() else {
            return;
        };
        let credentials = store.credentials(Keyring::from_bytes([6; 32]));
        let server = MockServer::start().await;
        Mock::given(path("/api/v1/auth/keys"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"key":"minted-key"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let login = OpenRouterLogin::with_base(&server.uri()).unwrap();
        let kind = login.login(&Ui).await.unwrap();
        credentials
            .put_entry(
                CredentialScope::Cluster,
                login.provider(),
                "default",
                &CredentialRecord {
                    kind,
                    updated_at: jiff::Timestamp::now(),
                },
            )
            .await
            .unwrap();
        let resolver = Resolver::new(Arc::new(ClusterCredentials::with_credentials(
            &store,
            credentials,
        )))
        .unwrap();
        assert!(
            matches!(resolver.resolve("openrouter").await.unwrap().auth, ClientAuth::ApiKey(key) if key == "minted-key")
        );
        let wrong = Resolver::new(Arc::new(ClusterCredentials::with_credentials(
            &store,
            store.credentials(Keyring::from_bytes([7; 32])),
        )))
        .unwrap();
        assert!(wrong.resolve("openrouter").await.is_err());
    }

    fn api_key(key: &str) -> CredentialRecord {
        CredentialRecord {
            kind: CredentialKind::ApiKey {
                key: key.into(),
                extra: BTreeMap::new(),
            },
            updated_at: jiff::Timestamp::now(),
        }
    }

    fn api_key_of(record: &CredentialRecord) -> &str {
        let CredentialKind::ApiKey { key, .. } = &record.kind else {
            panic!("expected an API key record")
        };
        key
    }

    struct StubLogin {
        calls: AtomicUsize,
        key: String,
        delay: Duration,
    }

    struct UnreachableLogin;

    #[async_trait]
    impl Login for UnreachableLogin {
        fn provider(&self) -> &'static str {
            "openai"
        }
        async fn login(&self, _: &dyn LoginUi) -> Result<CredentialKind, Error> {
            unreachable!("rotation already happened")
        }
        async fn refresh(&self, _: &CredentialKind) -> Result<Option<CredentialKind>, Error> {
            unreachable!("an adopted record never refreshes")
        }
    }

    #[async_trait]
    impl Login for StubLogin {
        fn provider(&self) -> &'static str {
            "openai"
        }
        async fn login(&self, _: &dyn LoginUi) -> Result<CredentialKind, Error> {
            Err(Error::Credentials(
                "interactive login is unavailable in tests",
            ))
        }
        async fn refresh(&self, _: &CredentialKind) -> Result<Option<CredentialKind>, Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            Ok(Some(api_key(&self.key).kind))
        }
    }

    #[tokio::test]
    async fn racing_entry_refreshers_adopt_the_winner() {
        let Some(store) = test_store() else {
            return;
        };
        let credentials = store.credentials(Keyring::from_bytes([9; 32]));
        let observed = api_key("old-key");
        credentials
            .put_entry(CredentialScope::Cluster, "openai", "default", &observed)
            .await
            .unwrap();
        let login = Arc::new(StubLogin {
            calls: AtomicUsize::new(0),
            key: "new-key".into(),
            delay: Duration::from_millis(150),
        });
        let first = ClusterCredentials::with_credentials(&store, credentials.clone());
        let second = ClusterCredentials::with_credentials(&store, credentials.clone());
        // Both gateways hold the stale record; one rotates while the other
        // adopts the winner instead of failing its turn.
        let (a, b) = tokio::join!(
            first.refresh("openai", &observed, login.as_ref()),
            second.refresh("openai", &observed, login.as_ref()),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert!(a == b);
        assert_eq!(api_key_of(&a), "new-key");
        assert_eq!(login.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stale_observer_adopts_an_externally_rotated_entry() {
        let Some(store) = test_store() else {
            return;
        };
        let credentials = store.credentials(Keyring::from_bytes([11; 32]));
        let observed = api_key("old-key");
        credentials
            .put_entry(CredentialScope::Cluster, "openai", "default", &observed)
            .await
            .unwrap();
        credentials
            .put_entry(
                CredentialScope::Cluster,
                "openai",
                "default",
                &api_key("rotated-key"),
            )
            .await
            .unwrap();
        let auth = ClusterCredentials::with_credentials(&store, credentials);
        let adopted = auth
            .refresh("openai", &observed, &UnreachableLogin)
            .await
            .unwrap();
        assert_eq!(api_key_of(&adopted), "rotated-key");
    }

    #[tokio::test]
    async fn legacy_record_reads_and_refreshes_through_default() {
        let Some(store) = test_store() else {
            return;
        };
        let credentials = store.credentials(Keyring::from_bytes([10; 32]));
        let legacy = api_key("legacy-key");
        credentials
            .put_credential(CredentialScope::Cluster, "openai", &legacy)
            .await
            .unwrap();
        let auth = ClusterCredentials::with_credentials(&store, credentials);
        // The first read migrates the legacy record; an old gateway sharing
        // the store would no longer see it, so gateways upgrade together.
        assert!(auth.get("openai").await.unwrap().unwrap() == legacy);
        let login = StubLogin {
            calls: AtomicUsize::new(0),
            key: "rotated-key".into(),
            delay: Duration::ZERO,
        };
        let rotated = auth.refresh("openai", &legacy, &login).await.unwrap();
        assert_eq!(api_key_of(&rotated), "rotated-key");
        assert_eq!(login.calls.load(Ordering::SeqCst), 1);
        assert!(auth.get("openai").await.unwrap().unwrap() == rotated);
    }
}
