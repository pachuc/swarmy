//! Cluster persistence for the provider-neutral credential resolver.
use async_trait::async_trait;
use std::time::Duration;
use swarmy_config::Keyring;
use swarmy_core::{CredentialRecord, CredentialScope};
use swarmy_llm::{
    Error,
    auth::{AuthStore, Login},
};
use swarmy_store::{Store, StoreError};

pub struct ClusterCredentials {
    store: swarmy_store::credentials::CredentialStore,
}

impl ClusterCredentials {
    pub async fn new(store: Store) -> anyhow::Result<Self> {
        let keyring = Keyring::load().map_err(|_| {
            anyhow::anyhow!(
                "gateway requires the cluster keyring; copy it to this host or set SWARMY_KEYRING"
            )
        })?;
        let store = store.credentials(keyring);
        // Diagnose wrong keys before accepting requests for any stored provider.
        store.list_credentials(CredentialScope::Cluster).await?;
        Ok(Self { store })
    }
}

#[async_trait]
impl AuthStore for ClusterCredentials {
    async fn get(&self, provider: &str) -> Result<Option<CredentialRecord>, Error> {
        self.store
            .get_credential(CredentialScope::Cluster, provider)
            .await
            .map_err(|_| {
                Error::Credentials("cluster credential unavailable; check keyring and database")
            })
    }

    async fn refresh(
        &self,
        provider: &str,
        observed: &CredentialRecord,
        login: &dyn Login,
    ) -> Result<CredentialRecord, Error> {
        self.store
            .refresh_with_lease(
                CredentialScope::Cluster,
                provider,
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
    use std::sync::{Arc, OnceLock};
    use swarmy_core::{CredentialKind, CredentialStatus};
    use swarmy_llm::{
        ClientAuth,
        auth::{Credentials, OAuthClient, Resolver},
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
            .put_credential(CredentialScope::Cluster, "chatgpt", &imported())
            .await
            .unwrap();
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/oauth/token")).respond_with(
            ResponseTemplate::new(200).set_delay(Duration::from_millis(150)).set_body_json(serde_json::json!({"access_token":"new-access", "refresh_token":"new-refresh"}))
        ).expect(1).mount(&server).await;
        let oauth = OAuthClient::with_issuer(&server.uri()).unwrap();
        let make = || {
            Resolver::with_chatgpt(
                Arc::new(ClusterCredentials {
                    store: credentials.clone(),
                }),
                oauth.clone(),
            )
        };
        let first = make();
        let second = make();
        let (a, b) = tokio::join!(first.resolve("chatgpt"), second.resolve("chatgpt"));
        for auth in [a.unwrap(), b.unwrap()] {
            let ClientAuth::ChatGpt(store) = auth else {
                panic!("expected ChatGPT");
            };
            assert_eq!(store.load().await.unwrap().access_token(), "new-access");
        }
        let ClientAuth::ChatGpt(provider_store) = first.resolve("chatgpt").await.unwrap() else {
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
            .get_credential(CredentialScope::Cluster, "chatgpt")
            .await
            .unwrap()
            .unwrap();
        let CredentialKind::OAuth { access, .. } = &mut updated.kind else {
            unreachable!()
        };
        *access = "externally-replaced".into();
        credentials
            .put_credential(CredentialScope::Cluster, "chatgpt", &updated)
            .await
            .unwrap();
        provider
            .request(request())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        credentials
            .delete_credential(CredentialScope::Cluster, "chatgpt")
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
            .put_credential(CredentialScope::Cluster, "chatgpt", &imported())
            .await
            .unwrap();
        let server = MockServer::start().await;
        Mock::given(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;
        let resolver = Resolver::with_chatgpt(
            Arc::new(ClusterCredentials {
                store: credentials.clone(),
            }),
            OAuthClient::with_issuer(&server.uri()).unwrap(),
        );
        for _ in 0..2 {
            assert!(matches!(
                resolver.resolve("chatgpt").await,
                Err(Error::NeedsLogin(_))
            ));
        }
        let record = credentials
            .get_credential(CredentialScope::Cluster, "chatgpt")
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
        use swarmy_llm::auth::{LoginUi, OpenRouterLogin};
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
            .put_credential(
                CredentialScope::Cluster,
                login.provider(),
                &CredentialRecord {
                    kind,
                    updated_at: jiff::Timestamp::now(),
                },
            )
            .await
            .unwrap();
        let resolver = Resolver::new(Arc::new(ClusterCredentials { store: credentials })).unwrap();
        assert!(
            matches!(resolver.resolve("openrouter").await.unwrap(), ClientAuth::ApiKey(key) if key == "minted-key")
        );
        let wrong = Resolver::new(Arc::new(ClusterCredentials {
            store: store.credentials(Keyring::from_bytes([7; 32])),
        }))
        .unwrap();
        assert!(wrong.resolve("openrouter").await.is_err());
    }
}
