//! Codex-compatible file credentials and the `ChatGPT` device-code flow.
use crate::Error;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

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
        for key in ["id_token", "access_token", "refresh_token", "account_id"] {
            credentials.token(key)?;
        }
        if account_from_id_token(credentials.token("id_token")?)? != credentials.account_id() {
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
                extra: [("chatgpt_json".into(), serde_json::to_string(&metadata)?)].into(),
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
        let mut value: Value = serde_json::from_str(
            extra
                .get("chatgpt_json")
                .ok_or(Error::Credentials("missing ChatGPT metadata"))?,
        )?;
        value["tokens"]["access_token"] = json!(access);
        value["tokens"]["refresh_token"] = json!(refresh);
        value["last_refresh"] = json!(record.updated_at.to_string());
        Self::from_json(value)
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
            return expiry <= now + 60;
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

/// Held across the refresh HTTP request and persistence. Dropping releases it.
pub trait CredentialLock: Send {}

/// Implementations must preserve account identity, atomically save credentials,
/// and serialize refresh across every user of their authoritative account store.
pub trait CredentialStore: Send + Sync {
    /// Store adapters may replace file locking with a database refresh lease.
    fn refresh<'a>(
        &'a self,
        client: &'a OAuthClient,
        observed: &'a Credentials,
    ) -> BoxFuture<'a, Result<Credentials, Error>> {
        Box::pin(client.refresh_locked(self, observed))
    }

    fn load(&self) -> BoxFuture<'_, Result<Credentials, Error>>;
    fn save(&self, credentials: Credentials) -> BoxFuture<'_, Result<(), Error>>;
    fn lock_refresh<'a>(
        &'a self,
        account_id: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialLock>, Error>>;
}

#[derive(Clone)]
pub struct FileCredentialStore {
    path: PathBuf,
    account_id: Arc<OnceLock<String>>,
}

impl FileCredentialStore {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            account_id: Arc::default(),
        }
    }

    fn check_account(&self, credentials: &Credentials) -> Result<(), Error> {
        if self
            .account_id
            .get_or_init(|| credentials.account_id().to_owned())
            != credentials.account_id()
        {
            return Err(Error::AccountChanged);
        }
        Ok(())
    }

    /// Import only reads the source. The source and copy share a refresh chain;
    /// the operator must stop the source's refresh owner before using the copy.
    /// # Errors
    /// Returns an error for invalid credentials, account changes, or file I/O.
    pub async fn import(&self, source: &Path) -> Result<(), Error> {
        if let Ok(destination) = tokio::fs::canonicalize(&self.path).await
            && destination == tokio::fs::canonicalize(source).await?
        {
            return Err(Error::Credentials(
                "import source and destination must differ",
            ));
        }
        let bytes = tokio::fs::read(source).await?;
        let credentials = Credentials::from_json(serde_json::from_slice(&bytes)?)?;
        let _lock = self.lock_refresh(credentials.account_id()).await?;
        self.save(credentials).await
    }
}

fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn private_directory(path: &Path) -> Result<(), Error> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    Ok(())
}

fn lock_file(path: &Path) -> Result<File, Error> {
    private_directory(parent(path))?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    fs2::FileExt::lock_exclusive(&file)?;
    Ok(file)
}

struct FileLock {
    _file: File,
    _local: OwnedMutexGuard<()>,
}
impl CredentialLock for FileLock {}

type AccountLocks = Mutex<HashMap<String, Weak<AsyncMutex<()>>>>;
static ACCOUNT_LOCKS: OnceLock<AccountLocks> = OnceLock::new();

fn account_lock(account: &str) -> Arc<AsyncMutex<()>> {
    let mut locks = ACCOUNT_LOCKS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(account).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(AsyncMutex::new(()));
    locks.insert(account.to_owned(), Arc::downgrade(&lock));
    lock
}

impl CredentialStore for FileCredentialStore {
    fn load(&self) -> BoxFuture<'_, Result<Credentials, Error>> {
        Box::pin(async {
            let credentials = Credentials::from_json(serde_json::from_slice(
                &tokio::fs::read(&self.path).await?,
            )?)?;
            self.check_account(&credentials)?;
            Ok(credentials)
        })
    }

    fn save(&self, credentials: Credentials) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.check_account(&credentials)?;
            let path = self.path.clone();
            tokio::task::spawn_blocking(move || {
                private_directory(parent(&path))?;
                let _write_lock = lock_file(&path.with_extension("write.lock"))?;
                match std::fs::read(&path) {
                    Ok(bytes) => {
                        let existing = Credentials::from_json(serde_json::from_slice(&bytes)?)?;
                        if existing.account_id() != credentials.account_id() {
                            return Err(Error::AccountChanged);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                    Err(error) => return Err(error.into()),
                }
                let mut temporary = tempfile::NamedTempFile::new_in(parent(&path))?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    temporary
                        .as_file()
                        .set_permissions(std::fs::Permissions::from_mode(0o600))?;
                }
                temporary.write_all(&serde_json::to_vec_pretty(credentials.to_json())?)?;
                temporary.as_file().sync_all()?;
                temporary.persist(&path).map_err(|error| error.error)?;
                File::open(parent(&path))?.sync_all()?;
                Ok(())
            })
            .await?
        })
    }

    fn lock_refresh<'a>(
        &'a self,
        account_id: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialLock>, Error>> {
        Box::pin(async move {
            let local = account_lock(account_id).lock_owned().await;
            let path = parent(&self.path).join(format!(
                ".refresh-{}.lock",
                blake3::hash(account_id.as_bytes()).to_hex()
            ));
            let file = tokio::task::spawn_blocking(move || lock_file(&path)).await??;
            Ok(Box::new(FileLock {
                _file: file,
                _local: local,
            }) as Box<dyn CredentialLock>)
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

    /// Poll for authorization, exchange the supplied PKCE verifier, and persist.
    /// # Errors
    /// Returns an error on timeout, failed login, account change, or persistence.
    pub async fn complete_login(
        &self,
        code: DeviceCode,
        store: &dyn CredentialStore,
    ) -> Result<(), Error> {
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
                let _lock = store.lock_refresh(credentials.account_id()).await?;
                return store.save(credentials).await;
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

    async fn refresh_locked<S: CredentialStore + ?Sized>(
        &self,
        store: &S,
        observed: &Credentials,
    ) -> Result<Credentials, Error> {
        let _lock = store.lock_refresh(observed.account_id()).await?;
        let current = store.load().await?;
        if current.account_id() != observed.account_id() {
            return Err(Error::AccountChanged);
        }
        if current != *observed {
            return Ok(current);
        }
        let refreshed = self.refresh_credentials(current).await?;
        store.save(refreshed.clone()).await?;
        Ok(refreshed)
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
        let refreshed = Credentials::from_json(value)?;
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

mod resolve;
pub use resolve::{ResolvedAuth, resolve};
