//! Resolve stored credentials before host environment and ambient cloud chains.
use super::{CredentialStore, Credentials, Login, OAuthClient, google, login_for};
use crate::{ClientAuth, Error};
use async_trait::async_trait;
use futures::future::BoxFuture;
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};
use swarmy_core::{CredentialKind, CredentialRecord, CredentialStatus};

/// The gateway supplies persistence so protocol clients do not link `FoundationDB`.
#[async_trait]
pub trait AuthStore: Send + Sync {
    /// # Errors
    /// Returns database and decryption errors.
    async fn get(&self, provider: &str) -> Result<Option<CredentialRecord>, Error>;
    /// The same record as `get` with its entry label, when the store holds
    /// labelled entries. The default delegates to `get` without a label.
    /// # Errors
    /// Returns database and decryption errors.
    async fn get_labelled(
        &self,
        provider: &str,
    ) -> Result<Option<(Option<String>, CredentialRecord)>, Error> {
        Ok(self.get(provider).await?.map(|record| (None, record)))
    }
    /// The one labelled entry a route step pins, without pool fallback. The
    /// default filters `get_labelled`; cluster stores read the entry directly.
    /// # Errors
    /// Returns database and decryption errors.
    async fn get_exact(
        &self,
        provider: &str,
        label: &str,
    ) -> Result<Option<CredentialRecord>, Error> {
        Ok(self
            .get_labelled(provider)
            .await?
            .and_then(|(entry, record)| {
                entry.is_some_and(|entry| entry == label).then_some(record)
            }))
    }
    /// Implementations must re-read under the lease and refresh only if unchanged.
    /// # Errors
    /// Returns lease, persistence, and provider refresh errors.
    async fn refresh(
        &self,
        provider: &str,
        observed: &CredentialRecord,
        login: &dyn Login,
    ) -> Result<CredentialRecord, Error>;
}

/// The version is opaque and must never be logged. It changes with credential content.
/// The entry is the stored label behind this resolution, used for usage attribution.
pub struct ResolvedAuth {
    pub auth: ClientAuth,
    pub version: [u8; 32],
    pub entry: Option<String>,
    /// Kind behind the resolved entry (`subscription`, `api-key`, `cloud`).
    pub entry_kind: Option<String>,
}

/// Derive the rollup kind from a stored record without exposing secrets.
#[must_use]
pub fn entry_kind_for(record: &CredentialRecord) -> String {
    match &record.kind {
        CredentialKind::OAuth { .. } => "subscription".into(),
        CredentialKind::ApiKey { extra, .. }
            if extra.get("auth_kind").is_some_and(|kind| kind == "cloud") =>
        {
            "cloud".into()
        }
        CredentialKind::ApiKey { .. } => "api-key".into(),
    }
}

#[derive(Clone)]
pub struct Resolver {
    store: Arc<dyn AuthStore>,
    chatgpt: OAuthClient,
}

impl Resolver {
    /// # Errors
    /// Returns HTTP client configuration errors.
    pub fn new(store: Arc<dyn AuthStore>) -> Result<Self, Error> {
        Ok(Self::with_chatgpt(store, OAuthClient::new()?))
    }

    /// Use a local issuer for protocol tests.
    #[must_use]
    pub fn with_chatgpt(store: Arc<dyn AuthStore>, chatgpt: OAuthClient) -> Self {
        Self { store, chatgpt }
    }

    /// Store first, then provider environment, then host SDK credentials.
    /// # Errors
    /// A stored credential failure is terminal; it never selects an environment key.
    pub async fn resolve(&self, provider: &str) -> Result<ResolvedAuth, Error> {
        self.resolve_using(provider, |name| std::env::var(name).ok())
            .await
    }

    /// The stored entry label the next resolution would use, if any. Used for
    /// providers that need no credentials to build a client (the fake
    /// provider) so their breaker is still keyed per entry. Unavailable
    /// stores resolve without a label.
    pub async fn entry_label(&self, provider: &str) -> Option<String> {
        self.store
            .get_labelled(provider)
            .await
            .ok()?
            .and_then(|(label, _)| label)
    }

    /// Resolve the one entry a route step pins, without pool fallback. A
    /// missing entry is an operator error, not a cue to try another key;
    /// failover already selected this step explicitly.
    /// # Errors
    /// Returns missing entries, credential failures, and refresh errors.
    pub async fn resolve_pinned(&self, provider: &str, label: &str) -> Result<ResolvedAuth, Error> {
        let record = self
            .store
            .get_exact(provider, label)
            .await?
            .ok_or_else(|| {
                Error::Credentials("route step names an entry with no stored credential")
            })?;
        let (entry, mut record) = (Some(label.to_owned()), record);
        if record.status(jiff::Timestamp::now()) == CredentialStatus::NeedsLogin {
            return Err(Error::NeedsLogin(provider.into()));
        }
        if provider == "anthropic" && matches!(record.kind, CredentialKind::OAuth { .. }) {
            return Err(Error::Credentials("Anthropic requires an API key"));
        }
        if provider == "amazon-bedrock"
            && record.status(jiff::Timestamp::now()) == CredentialStatus::Expired
        {
            return Err(Error::Credentials(
                "Bedrock console API keys expire after twelve hours and are for development only; use an IAM identity for long-lived use",
            ));
        }
        if record.needs_refresh(jiff::Timestamp::now()) {
            let login: Box<dyn Login> = if provider == "chatgpt" {
                Box::new(self.chatgpt.clone())
            } else {
                login_for(provider, None, None)?
            };
            record = self
                .store
                .refresh(provider, &record, login.as_ref())
                .await?;
        }
        if record.status(jiff::Timestamp::now()) != CredentialStatus::Ready {
            return Err(Error::NeedsLogin(provider.into()));
        }
        if provider == "chatgpt" {
            let credentials = Credentials::from_record(&record)?;
            let account = OnceLock::new();
            let _ = account.set(credentials.account_id().into());
            return Ok(ResolvedAuth {
                auth: ClientAuth::ChatGpt(Arc::new(ChatGptCredentials {
                    resolver: self.clone(),
                    account,
                })),
                version: version(&record)?,
                entry,
                entry_kind: Some(entry_kind_for(&record)),
            });
        }
        let version = version(&record)?;
        let entry_kind = Some(entry_kind_for(&record));
        let auth = if is_vertex(provider) {
            vertex_from_record(record.kind)
        } else {
            auth_from_kind(record.kind)?
        };
        Ok(ResolvedAuth {
            auth,
            version,
            entry,
            entry_kind,
        })
    }

    async fn resolve_using(
        &self,
        provider: &str,
        environment_value: impl Fn(&str) -> Option<String>,
    ) -> Result<ResolvedAuth, Error> {
        let (entry, record) = self.record(provider).await?;
        if provider == "chatgpt" {
            let record = record.ok_or_else(|| Error::NeedsLogin(provider.into()))?;
            let credentials = Credentials::from_record(&record)?;
            let account = OnceLock::new();
            let _ = account.set(credentials.account_id().into());
            return Ok(ResolvedAuth {
                auth: ClientAuth::ChatGpt(Arc::new(ChatGptCredentials {
                    resolver: self.clone(),
                    account,
                })),
                version: version(&record)?,
                entry_kind: Some(entry_kind_for(&record)),
                entry,
            });
        }
        if let Some(record) = record {
            let version = version(&record)?;
            let kind = entry_kind_for(&record);
            let auth = if is_vertex(provider) {
                vertex_from_record(record.kind)
            } else {
                auth_from_kind(record.kind)?
            };
            return Ok(ResolvedAuth {
                auth,
                version,
                entry,
                entry_kind: Some(kind),
            });
        }
        environment(provider, environment_value)
    }

    async fn record(
        &self,
        provider: &str,
    ) -> Result<(Option<String>, Option<CredentialRecord>), Error> {
        let Some((label, mut record)) = self.store.get_labelled(provider).await? else {
            return Ok((None, None));
        };
        if record.status(jiff::Timestamp::now()) == CredentialStatus::NeedsLogin {
            return Err(Error::NeedsLogin(provider.into()));
        }
        if provider == "anthropic" && matches!(record.kind, CredentialKind::OAuth { .. }) {
            return Err(Error::Credentials("Anthropic requires an API key"));
        }
        if provider == "amazon-bedrock"
            && record.status(jiff::Timestamp::now()) == CredentialStatus::Expired
        {
            return Err(Error::Credentials(
                "Bedrock console API keys expire after twelve hours and are for development only; use an IAM identity for long-lived use",
            ));
        }
        if record.needs_refresh(jiff::Timestamp::now()) {
            let login: Box<dyn Login> = if provider == "chatgpt" {
                Box::new(self.chatgpt.clone())
            } else {
                login_for(provider, None, None)?
            };
            record = self
                .store
                .refresh(provider, &record, login.as_ref())
                .await?;
        }
        if record.status(jiff::Timestamp::now()) != CredentialStatus::Ready {
            return Err(Error::NeedsLogin(provider.into()));
        }
        Ok((label, Some(record)))
    }
}

/// Resolve using an explicitly supplied cluster context, without process globals.
/// # Errors
/// Returns credential, refresh, and persistence errors.
pub async fn resolve(provider: &str, resolver: &Resolver) -> Result<ResolvedAuth, Error> {
    resolver.resolve(provider).await
}

/// Stored credentials version with their serialized content, so rotation
/// retires cached clients without exposing the secret itself.
fn version(record: &CredentialRecord) -> Result<[u8; 32], Error> {
    Ok(*blake3::hash(&serde_json::to_vec(record)?).as_bytes())
}

fn is_vertex(provider: &str) -> bool {
    matches!(provider, "google-vertex" | "google-vertex-anthropic")
}

/// Stored Vertex records carry project, location, and credential extras for the
/// shared Google builder; a bare secret is an explicit access token. Records the
/// builder cannot use leave the host SDK chain to the protocol client.
fn vertex_from_record(kind: CredentialKind) -> ClientAuth {
    let (secret, mut extra) = match kind {
        CredentialKind::ApiKey { key, extra } => (key, extra),
        CredentialKind::OAuth { access, extra, .. } => (access, extra),
    };
    if !secret.is_empty()
        && !extra.contains_key("service_account_json")
        && !extra.contains_key("access_token")
    {
        extra.insert("access_token".into(), secret);
    }
    google::vertex_auth(&extra).unwrap_or(ClientAuth::Ambient)
}

fn auth_from_kind(kind: CredentialKind) -> Result<ClientAuth, Error> {
    match kind {
        CredentialKind::ApiKey { key, extra } => {
            if key.is_empty() {
                return Err(Error::Credentials("empty API key"));
            }
            if extra.is_empty() {
                Ok(ClientAuth::ApiKey(key))
            } else {
                Ok(ClientAuth::ApiKeyWithExtra { key, extra })
            }
        }
        CredentialKind::OAuth { access, extra, .. } => {
            if extra.is_empty() {
                Ok(ClientAuth::Bearer(access))
            } else {
                Ok(ClientAuth::BearerWithExtra {
                    token: access,
                    extra,
                })
            }
        }
    }
}

fn environment(
    provider: &str,
    get: impl Fn(&str) -> Option<String>,
) -> Result<ResolvedAuth, Error> {
    let ambient = |auth| {
        Ok(ResolvedAuth {
            auth,
            version: [0; 32],
            entry: None,
            entry_kind: None,
        })
    };
    if provider == "fake" {
        return ambient(ClientAuth::None);
    }
    // Catalog env_keys also lists endpoint and SDK settings. Only these are keys.
    let key_names: &[&str] = match provider {
        "anthropic" => &["ANTHROPIC_API_KEY"],
        "openai" => &["OPENAI_API_KEY"],
        "xai" => &["XAI_API_KEY"],
        "meta" => &["META_MODEL_API_KEY"],
        "openrouter" => &["OPENROUTER_API_KEY"],
        "azure" => &["AZURE_API_KEY", "AZURE_OPENAI_API_KEY"],
        "google" => &[
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
            "GOOGLE_GENERATIVE_AI_API_KEY",
        ],
        "amazon-bedrock" => &["AWS_BEARER_TOKEN_BEDROCK"],
        _ => &[],
    };
    let catalog = crate::catalog::Catalog::get();
    let info = catalog
        .provider(provider)
        .ok_or(Error::Credentials("unknown provider"))?;
    let key = key_names
        .iter()
        .filter(|name| info.env_keys.iter().any(|env| env == **name))
        .find_map(|name| get(name).filter(|value| !value.trim().is_empty()));
    if let Some(key) = key {
        let version = *blake3::hash(key.as_bytes()).as_bytes();
        let auth = if provider == "azure" {
            let mut extra = BTreeMap::new();
            if let Some(resource) = get("AZURE_RESOURCE_NAME").filter(|s| !s.trim().is_empty()) {
                extra.insert("resource_name".into(), resource);
            }
            if let Some(endpoint) = get("AZURE_OPENAI_BASE_URL").filter(|s| !s.trim().is_empty()) {
                extra.insert("base_url".into(), endpoint);
            }
            if extra.is_empty() {
                return Err(Error::Credentials(
                    "Azure requires AZURE_RESOURCE_NAME or AZURE_OPENAI_BASE_URL",
                ));
            }
            ClientAuth::ApiKeyWithExtra { key, extra }
        } else if provider == "amazon-bedrock" {
            ClientAuth::Bearer(key)
        } else {
            ClientAuth::ApiKey(key)
        };
        return Ok(ResolvedAuth {
            auth,
            version,
            entry: None,
            entry_kind: None,
        });
    }
    if is_vertex(provider) {
        // Host Google credentials build the shared Vertex auth; without them the
        // protocol client is left to the SDK chain.
        return match google::vertex_auth_with(&BTreeMap::new(), &get) {
            Ok(auth) => {
                let mut hasher = blake3::Hasher::new();
                for name in &info.env_keys {
                    hasher.update(get(name).unwrap_or_default().as_bytes());
                    hasher.update(&[0]);
                }
                Ok(ResolvedAuth {
                    auth,
                    version: *hasher.finalize().as_bytes(),
                    entry: None,
                    entry_kind: None,
                })
            }
            Err(_) => ambient(ClientAuth::Ambient),
        };
    }
    if provider == "amazon-bedrock" {
        return ambient(ClientAuth::Ambient);
    }
    Err(Error::NeedsLogin(provider.into()))
}

struct ChatGptCredentials {
    resolver: Resolver,
    account: OnceLock<String>,
}

impl ChatGptCredentials {
    fn check(&self, record: &CredentialRecord) -> Result<Credentials, Error> {
        let credentials = Credentials::from_record(record)?;
        if self.account.get_or_init(|| credentials.account_id().into()) != credentials.account_id()
        {
            return Err(Error::AccountChanged);
        }
        Ok(credentials)
    }
}

impl CredentialStore for ChatGptCredentials {
    fn load(&self) -> BoxFuture<'_, Result<Credentials, Error>> {
        Box::pin(async {
            let record = self
                .resolver
                .record("chatgpt")
                .await?
                .1
                .ok_or_else(|| Error::NeedsLogin("chatgpt".into()))?;
            self.check(&record)
        })
    }
    fn refresh<'a>(
        &'a self,
        client: &'a OAuthClient,
        observed: &'a Credentials,
    ) -> BoxFuture<'a, Result<Credentials, Error>> {
        Box::pin(async move {
            let record = self
                .resolver
                .store
                .get("chatgpt")
                .await?
                .ok_or_else(|| Error::NeedsLogin("chatgpt".into()))?;
            let current = self.check(&record)?;
            if current != *observed {
                return Ok(current);
            }
            self.check(
                &self
                    .resolver
                    .store
                    .refresh("chatgpt", &record, client)
                    .await?,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn environment_keys_metadata_and_ambient() {
        let resolved = environment("openai", |_| Some("env-key".into())).unwrap();
        assert!(matches!(resolved.auth, ClientAuth::ApiKey(key) if key == "env-key"));
        assert_ne!(resolved.version, [0; 32]);
        assert_ne!(
            resolved.version,
            environment("openai", |_| Some("rotated".into()))
                .unwrap()
                .version
        );
        for provider in ["amazon-bedrock", "google-vertex", "google-vertex-anthropic"] {
            let resolved = environment(provider, |key| {
                (key != "AWS_BEARER_TOKEN_BEDROCK").then(|| "configuration".into())
            })
            .unwrap();
            assert!(matches!(resolved.auth, ClientAuth::Ambient));
            assert_eq!(resolved.version, [0; 32]);
        }
        assert!(matches!(
            environment("amazon-bedrock", |key| (key == "AWS_BEARER_TOKEN_BEDROCK")
                .then(|| "token".into()))
            .unwrap()
            .auth,
            ClientAuth::Bearer(_)
        ));
        assert!(
            matches!(environment("azure", |name| Some(name.into())).unwrap().auth, ClientAuth::ApiKeyWithExtra { key, extra } if key == "AZURE_API_KEY" && extra["resource_name"] == "AZURE_RESOURCE_NAME")
        );
        assert!(
            environment("azure", |name| (name == "AZURE_API_KEY")
                .then(|| "key".into()))
            .is_err()
        );
    }
    struct Store {
        record: Option<CredentialRecord>,
    }
    #[async_trait]
    impl AuthStore for Store {
        async fn get(&self, _: &str) -> Result<Option<CredentialRecord>, Error> {
            Ok(self.record.clone())
        }
        async fn refresh(
            &self,
            provider: &str,
            _: &CredentialRecord,
            _: &dyn Login,
        ) -> Result<CredentialRecord, Error> {
            Err(Error::NeedsLogin(provider.into()))
        }
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

    fn resolver(record: Option<CredentialRecord>) -> Resolver {
        Resolver::new(Arc::new(Store { record })).unwrap()
    }

    #[tokio::test]
    async fn expired_bedrock_record_explains_iam_alternative() {
        let record = CredentialRecord {
            kind: CredentialKind::OAuth {
                access: "expired".into(),
                refresh: "unusable".into(),
                expires_at: jiff::Timestamp::from_second(1).unwrap(),
                extra: BTreeMap::new(),
            },
            updated_at: jiff::Timestamp::now(),
        };
        let error = resolver(Some(record))
            .resolve_using("amazon-bedrock", |_| {
                panic!("stored records must not fall back")
            })
            .await;
        assert!(
            matches!(error, Err(Error::Credentials(message)) if message.contains("IAM identity"))
        );
    }

    #[tokio::test]
    async fn vertex_credentials_build_shared_auth_before_ambient() {
        let adc = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            adc.path(),
            r#"{"client_id":"id","client_secret":"secret","refresh_token":"refresh"}"#,
        )
        .unwrap();
        let path = adc.path().to_str().unwrap().to_owned();
        for provider in ["google-vertex", "google-vertex-anthropic"] {
            let resolved = environment(provider, |name| match name {
                "GOOGLE_APPLICATION_CREDENTIALS" => Some(path.clone()),
                "GOOGLE_CLOUD_PROJECT" => Some("proj".into()),
                _ => None,
            })
            .unwrap();
            assert!(matches!(
                &resolved.auth,
                ClientAuth::Vertex { project, location, .. }
                    if project == "proj" && location == "us-central1"
            ));
            assert_ne!(resolved.version, [0; 32]);
            // Without a project the builder cannot resolve, so the SDK chain remains.
            let resolved = environment(provider, |name| {
                (name == "GOOGLE_APPLICATION_CREDENTIALS").then(|| path.clone())
            })
            .unwrap();
            assert!(matches!(resolved.auth, ClientAuth::Ambient));
        }
        let record = CredentialRecord {
            kind: CredentialKind::ApiKey {
                key: "ya29.token".into(),
                extra: BTreeMap::from([
                    ("project".into(), "proj".into()),
                    ("location".into(), "global".into()),
                ]),
            },
            updated_at: jiff::Timestamp::now(),
        };
        let resolved = resolver(Some(record))
            .resolve_using("google-vertex", |_| {
                panic!("stored records never inspect environment")
            })
            .await
            .unwrap();
        assert!(
            matches!(resolved.auth, ClientAuth::Vertex { location, .. } if location == "global")
        );
        let resolved = environment("google", |name| {
            (name == "GEMINI_API_KEY").then(|| "key".into())
        })
        .unwrap();
        assert!(matches!(resolved.auth, ClientAuth::ApiKey(_)));
    }

    #[tokio::test]
    async fn store_precedence_absence_and_failed_refresh() {
        let stored = resolver(Some(api_key("stored")))
            .resolve_using("openai", |_| Some("environment".into()))
            .await
            .unwrap();
        assert!(matches!(stored.auth, ClientAuth::ApiKey(key) if key == "stored"));
        let rotated = resolver(Some(api_key("rotated")))
            .resolve_using("openai", |_| None)
            .await
            .unwrap();
        assert_ne!(stored.version, rotated.version);
        assert!(
            resolver(Some(api_key("")))
                .resolve_using("openai", |_| Some("environment".into()))
                .await
                .is_err()
        );
        assert!(
            matches!(resolver(None).resolve_using("openai", |_| Some("environment".into())).await.unwrap().auth, ClientAuth::ApiKey(key) if key == "environment")
        );
        let record = CredentialRecord {
            kind: CredentialKind::OAuth {
                access: "old".into(),
                refresh: "refresh".into(),
                expires_at: jiff::Timestamp::now(),
                extra: BTreeMap::new(),
            },
            updated_at: jiff::Timestamp::now(),
        };
        assert!(matches!(
            resolver(Some(record))
                .resolve_using("azure", |_| panic!(
                    "stored refresh failure must not inspect environment"
                ))
                .await,
            Err(Error::NeedsLogin(_))
        ));
    }
}
