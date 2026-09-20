//! Codex-compatible file credentials and the `ChatGPT` device-code flow.
use crate::Error;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::OnceLock, time::Duration};

pub const AUTH_ISSUER: &str = "https://auth.openai.com";
pub const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";

/// Retains all unknown fields without exposing secrets through `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials(Value);

impl Credentials {
    /// # Errors
    /// Rejects API keys, missing tokens, invalid timestamps, and account mismatches.
    pub fn from_json(value: Value) -> Result<Self, Error> {
        if value["auth_mode"] != "chatgpt" || !value["OPENAI_API_KEY"].is_null() {
            return Err(Error::Credentials(
                "only ChatGPT OAuth credentials are accepted",
            ));
        }
        let credentials = Self(value);
        credentials.token("id_token")?;
        credentials.validate()
    }

    fn validate(self) -> Result<Self, Error> {
        let credentials = self;
        for key in ["access_token", "refresh_token", "account_id"] {
            credentials.token(key)?;
        }
        if !credentials.0["tokens"]["id_token"].is_null()
            && account_from_id_token(credentials.token("id_token")?)? != credentials.account_id()
        {
            return Err(Error::AccountChanged);
        }
        credentials.0["last_refresh"]
            .as_str()
            .ok_or(Error::Credentials("missing last_refresh"))?
            .parse::<jiff::Timestamp>()
            .map_err(|_| Error::Credentials("invalid last_refresh"))?;
        Ok(credentials)
    }

    #[must_use]
    pub fn to_json(&self) -> &Value {
        &self.0
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        // Construction validates this field and the raw value is immutable.
        self.0["tokens"]["account_id"].as_str().unwrap_or_default()
    }

    #[must_use]
    pub fn access_token(&self) -> &str {
        self.0["tokens"]["access_token"]
            .as_str()
            .unwrap_or_default()
    }

    /// Convert the existing file format while retaining unknown provider fields.
    /// # Errors
    /// Returns invalid timestamp or token errors.
    pub fn to_record(&self) -> Result<swarmy_core::CredentialRecord, Error> {
        let updated_at: jiff::Timestamp = self.0["last_refresh"]
            .as_str()
            .unwrap_or_default()
            .parse()
            .map_err(|_| Error::Credentials("invalid last_refresh"))?;
        let expires_at = jwt_claims(self.access_token())
            .ok()
            .and_then(|v| v["exp"].as_i64())
            .and_then(|v| jiff::Timestamp::from_second(v).ok())
            .unwrap_or(
                updated_at
                    .checked_add(Duration::from_hours(192))
                    .map_err(|_| Error::Credentials("invalid expiry"))?,
            );
        let mut metadata = self.0.clone();
        metadata["tokens"]
            .as_object_mut()
            .ok_or(Error::Credentials("missing tokens"))?
            .remove("access_token");
        metadata["tokens"]
            .as_object_mut()
            .ok_or(Error::Credentials("missing tokens"))?
            .remove("refresh_token");
        Ok(swarmy_core::CredentialRecord {
            kind: swarmy_core::CredentialKind::OAuth {
                access: self.access_token().into(),
                refresh: self.token("refresh_token")?.into(),
                expires_at,
                extra: [
                    ("chatgpt_json".into(), serde_json::to_string(&metadata)?),
                    ("account_id".into(), self.account_id().into()),
                ]
                .into(),
            },
            updated_at,
        })
    }

    /// # Errors
    /// Rejects non-OAuth records, missing metadata, and invalid account identities.
    pub fn from_record(record: &swarmy_core::CredentialRecord) -> Result<Self, Error> {
        let swarmy_core::CredentialKind::OAuth {
            access,
            refresh,
            extra,
            ..
        } = &record.kind
        else {
            return Err(Error::Credentials("ChatGPT requires OAuth"));
        };
        if extra.get("needs_login").is_some_and(|v| v == "true") {
            return Err(Error::Credentials("ChatGPT needs login"));
        }
        let mut value: Value = if let Some(metadata) = extra.get("chatgpt_json") {
            serde_json::from_str(metadata)?
        } else {
            json!({"auth_mode":"chatgpt", "OPENAI_API_KEY":null, "tokens": {
                "account_id": extra.get("account_id").ok_or(Error::Credentials("missing ChatGPT account_id"))?
            }})
        };
        if extra
            .get("account_id")
            .is_some_and(|account| value["tokens"]["account_id"].as_str() != Some(account))
        {
            return Err(Error::AccountChanged);
        }
        value["tokens"]["access_token"] = json!(access);
        value["tokens"]["refresh_token"] = json!(refresh);
        value["last_refresh"] = json!(record.updated_at.to_string());
        Self(value).validate()
    }

    fn token(&self, key: &str) -> Result<&str, Error> {
        self.0["tokens"][key]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(Error::Credentials("missing token field"))
    }

    pub(crate) fn needs_refresh(&self) -> bool {
        let now = jiff::Timestamp::now().as_second();
        if let Ok(claims) = jwt_claims(self.access_token())
            && let Some(expiry) = claims["exp"].as_i64()
        {
            return expiry < now + 300;
        }
        let refreshed = self.0["last_refresh"]
            .as_str()
            .and_then(|s| s.parse::<jiff::Timestamp>().ok());
        refreshed.is_none_or(|time| now - time.as_second() >= 8 * 24 * 60 * 60)
    }
}

fn jwt_claims(token: &str) -> Result<Value, Error> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or(Error::Credentials("invalid JWT"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| Error::Credentials("invalid JWT encoding"))?;
    serde_json::from_slice(&bytes).map_err(|_| Error::Credentials("invalid JWT claims"))
}

fn account_from_id_token(token: &str) -> Result<String, Error> {
    jwt_claims(token)?["https://api.openai.com/auth"]["chatgpt_account_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or(Error::Credentials("missing account claim"))
}

/// A live `ChatGPT` source reloads tokens and fences refresh through its authority.
pub trait CredentialStore: Send + Sync {
    fn load(&self) -> BoxFuture<'_, Result<Credentials, Error>>;
    fn refresh<'a>(
        &'a self,
        _client: &'a OAuthClient,
        _observed: &'a Credentials,
    ) -> BoxFuture<'a, Result<Credentials, Error>> {
        Box::pin(async {
            Err(Error::Credentials(
                "import the credential file before refreshing",
            ))
        })
    }
}

/// Read-only compatibility with the old file format for `auth import`.
pub struct FileCredentialStore {
    path: PathBuf,
    account_id: OnceLock<String>,
}

impl FileCredentialStore {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            account_id: OnceLock::new(),
        }
    }
}

impl CredentialStore for FileCredentialStore {
    fn load(&self) -> BoxFuture<'_, Result<Credentials, Error>> {
        Box::pin(async {
            let credentials = Credentials::from_json(serde_json::from_slice(
                &tokio::fs::read(&self.path).await?,
            )?)?;
            if self
                .account_id
                .get_or_init(|| credentials.account_id().into())
                != credentials.account_id()
            {
                return Err(Error::AccountChanged);
            }
            Ok(credentials)
        })
    }
}

#[derive(Clone)]
pub struct OAuthClient {
    client: reqwest::Client,
    issuer: String,
}

/// Only the URL and short code are public; callers display them before polling.
pub struct DeviceCode {
    pub verification_url: String,
    pub user_code: String,
    device_auth_id: String,
    interval: Duration,
}

impl OAuthClient {
    /// # Errors
    /// Returns an error if the HTTP client cannot be configured.
    pub fn new() -> Result<Self, Error> {
        Self::with_issuer(AUTH_ISSUER)
    }

    /// Override the issuer for a local protocol test server.
    /// # Errors
    /// Returns an error if the HTTP client cannot be configured.
    pub fn with_issuer(issuer: &str) -> Result<Self, Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent(concat!("swarmy/", env!("CARGO_PKG_VERSION")))
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(30))
                .build()?,
            issuer: issuer.trim_end_matches('/').to_owned(),
        })
    }

    /// # Errors
    /// Returns an error if device login is unavailable or the reply is malformed.
    pub async fn device_code(&self) -> Result<DeviceCode, Error> {
        let response = self
            .client
            .post(format!("{}/api/accounts/deviceauth/usercode", self.issuer))
            .json(&json!({"client_id": CLIENT_ID}))
            .send()
            .await?;
        let value = checked_json(response).await?;
        let interval = value["interval"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| value["interval"].as_u64())
            .unwrap_or(5)
            .max(1);
        Ok(DeviceCode {
            verification_url: format!("{}/codex/device", self.issuer),
            user_code: value["user_code"]
                .as_str()
                .or_else(|| value["usercode"].as_str())
                .ok_or(Error::Credentials("missing user code"))?
                .to_owned(),
            device_auth_id: required(&value, "device_auth_id")?.to_owned(),
            interval: Duration::from_secs(interval),
        })
    }

    /// Complete the device flow without choosing a persistence backend.
    /// # Errors
    /// Returns login, HTTP, timeout, or account validation errors.
    pub async fn exchange_device_code(&self, code: DeviceCode) -> Result<Credentials, Error> {
        tokio::time::timeout(Duration::from_mins(15), async {
            loop {
                let response = self.client.post(format!("{}/api/accounts/deviceauth/token", self.issuer))
                    .json(&json!({"device_auth_id": code.device_auth_id, "user_code": code.user_code})).send().await?;
                if matches!(response.status().as_u16(), 403 | 404) {
                    tokio::time::sleep(code.interval).await;
                    continue;
                }
                let value = checked_json(response).await?;
                let response = self.client.post(format!("{}/oauth/token", self.issuer)).form(&[
                    ("grant_type", "authorization_code"), ("client_id", CLIENT_ID),
                    ("code", required(&value, "authorization_code")?),
                    ("code_verifier", required(&value, "code_verifier")?),
                    ("redirect_uri", &format!("{}/deviceauth/callback", self.issuer)),
                ]).send().await?;
                let tokens = checked_json(response).await?;
                let account_id = account_from_id_token(required(&tokens, "id_token")?)?;
                let credentials = Credentials::from_json(json!({
                    "auth_mode": "chatgpt", "OPENAI_API_KEY": null,
                    "tokens": {"id_token": required(&tokens, "id_token")?, "access_token": required(&tokens, "access_token")?, "refresh_token": required(&tokens, "refresh_token")?, "account_id": account_id},
                    "last_refresh": jiff::Timestamp::now().to_string(),
                }))?;
                return Ok(credentials);
            }
        }).await.map_err(|_| Error::LoginTimeout)?
    }

    /// Refresh only if the observed credentials are still current after locking.
    /// Never retries a refresh: a lost reply may already have rotated the chain.
    /// # Errors
    /// Returns errors for HTTP failures, account changes, or persistence failures.
    pub async fn refresh(
        &self,
        store: &dyn CredentialStore,
        observed: &Credentials,
    ) -> Result<Credentials, Error> {
        store.refresh(self, observed).await
    }

    /// Exchange one refresh token. The caller must hold its authoritative lease.
    /// # Errors
    /// Returns HTTP, malformed reply, and account identity errors.
    pub async fn refresh_credentials(&self, current: Credentials) -> Result<Credentials, Error> {
        let account_id = current.account_id().to_owned();
        let response = self.client.post(format!("{}/oauth/token", self.issuer)).json(&json!({
            "grant_type": "refresh_token", "client_id": CLIENT_ID, "refresh_token": current.token("refresh_token")?,
        })).send().await?;
        let update = checked_json(response).await?;
        required(&update, "access_token")?;
        let mut value = current.0;
        for key in ["id_token", "access_token", "refresh_token"] {
            if !update[key].is_null() {
                value["tokens"][key] = update[key].clone();
            }
        }
        value["last_refresh"] = json!(jiff::Timestamp::now().to_string());
        let refreshed = Credentials(value).validate()?;
        if refreshed.account_id() != account_id {
            return Err(Error::AccountChanged);
        }
        Ok(refreshed)
    }
}

fn required<'a>(value: &'a Value, key: &str) -> Result<&'a str, Error> {
    value[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or(Error::Credentials("missing OAuth response field"))
}

async fn checked_json(response: reqwest::Response) -> Result<Value, Error> {
    if !response.status().is_success() {
        return Err(Error::Status(response.status()));
    }
    Ok(response.json().await?)
}
