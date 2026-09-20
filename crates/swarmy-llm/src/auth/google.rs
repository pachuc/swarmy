//! Google service account and application-default bearer credentials.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use futures::future::BoxFuture;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::{BearerSource, ClientAuth, Error};

const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

enum Credential {
    Access(String),
    ServiceAccount {
        email: String,
        key: EncodingKey,
        audience: String,
    },
    Refresh {
        client_id: String,
        client_secret: String,
        refresh_token: String,
    },
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
}

struct CachedToken {
    access: String,
    expires_at: i64,
}

/// Secrets are never included in debug output. Concurrent requests share refreshes.
pub struct GoogleBearerSource {
    credential: Credential,
    client: reqwest::Client,
    endpoint: String,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    cached: Mutex<Option<CachedToken>>,
}

impl GoogleBearerSource {
    /// Resolve explicit credentials before the environment and gcloud's ADC file.
    /// # Errors
    /// Returns an error for absent or malformed credentials and unreadable files.
    pub fn resolve(extra: &BTreeMap<String, String>) -> Result<Self, Error> {
        Self::resolve_with(extra, |name| std::env::var(name).ok())
    }

    /// [`Self::resolve`] over a caller-supplied environment lookup.
    /// # Errors
    /// Returns an error for absent or malformed credentials and unreadable files.
    pub fn resolve_with(
        extra: &BTreeMap<String, String>,
        environment: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, Error> {
        let explicit = environment("GOOGLE_APPLICATION_CREDENTIALS").map(PathBuf::from);
        let default = environment("HOME").map(|home| {
            PathBuf::from(home).join(".config/gcloud/application_default_credentials.json")
        });
        Self::resolve_paths(extra, explicit, default)
    }

    fn resolve_paths(
        extra: &BTreeMap<String, String>,
        environment: Option<PathBuf>,
        default: Option<PathBuf>,
    ) -> Result<Self, Error> {
        let credential = if let Some(json) = extra.get("service_account_json") {
            Self::parse(&serde_json::from_str::<Value>(json)?)?
        } else if let Some(token) = extra.get("access_token") {
            if token.is_empty() {
                return Err(Error::Credentials("empty Google access token"));
            }
            Credential::Access(token.clone())
        } else {
            let path = environment
                .or(default)
                .ok_or(Error::Credentials("Google ADC file not found"))?;
            Self::parse(&serde_json::from_slice::<Value>(&std::fs::read(path)?)?)?
        };
        Ok(Self {
            credential,
            client: reqwest::Client::builder()
                .user_agent(concat!("swarmy/", env!("CARGO_PKG_VERSION")))
                .timeout(std::time::Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint: TOKEN_URL.into(),
            clock: Arc::new(|| jiff::Timestamp::now().as_second()),
            cached: Mutex::new(None),
        })
    }

    fn parse(value: &Value) -> Result<Credential, Error> {
        let field = |name: &str| -> Result<String, Error> {
            value[name]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .ok_or(Error::Credentials("missing Google credential field"))
        };
        if value.get("private_key").is_some() {
            Ok(Credential::ServiceAccount {
                email: field("client_email")?,
                key: EncodingKey::from_rsa_pem(field("private_key")?.as_bytes())
                    .map_err(|_| Error::Credentials("invalid Google RSA private key"))?,
                audience: field("token_uri")?,
            })
        } else {
            Ok(Credential::Refresh {
                client_id: field("client_id")?,
                client_secret: field("client_secret")?,
                refresh_token: field("refresh_token")?,
            })
        }
    }

    async fn access_token(&self) -> Result<String, Error> {
        if let Credential::Access(token) = &self.credential {
            return Ok(token.clone());
        }
        let mut cached = self.cached.lock().await;
        let now = (self.clock)();
        if let Some(token) = cached
            .as_ref()
            .filter(|t| t.expires_at > now.saturating_add(60))
        {
            return Ok(token.access.clone());
        }
        let form = match &self.credential {
            Credential::ServiceAccount {
                email,
                key,
                audience,
            } => {
                let claims = json!({"iss": email, "scope": SCOPE, "aud": audience, "iat": now, "exp": now.saturating_add(3600)});
                let assertion = jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, key)
                    .map_err(|_| Error::Credentials("Google JWT signing failed"))?;
                vec![
                    (
                        "grant_type",
                        "urn:ietf:params:oauth:grant-type:jwt-bearer".into(),
                    ),
                    ("assertion", assertion),
                ]
            }
            Credential::Refresh {
                client_id,
                client_secret,
                refresh_token,
            } => vec![
                ("grant_type", "refresh_token".into()),
                ("client_id", client_id.clone()),
                ("client_secret", client_secret.clone()),
                ("refresh_token", refresh_token.clone()),
            ],
            Credential::Access(_) => unreachable!(),
        };
        let response = self.client.post(&self.endpoint).form(&form).send().await?;
        if !response.status().is_success() {
            return Err(Error::Status(response.status()));
        }
        let token: TokenResponse = response.json().await?;
        if token.access_token.is_empty() || token.expires_in <= 0 {
            return Err(Error::Credentials("invalid Google token response"));
        }
        *cached = Some(CachedToken {
            access: token.access_token.clone(),
            expires_at: now.saturating_add(token.expires_in),
        });
        Ok(token.access_token)
    }
}

impl BearerSource for GoogleBearerSource {
    fn token(&self) -> BoxFuture<'_, Result<String, Error>> {
        Box::pin(self.access_token())
    }
}

/// Build the shared Vertex auth variant for Gemini or Anthropic from credential extras.
/// # Errors
/// Returns an error if the project or Google credentials cannot be resolved.
pub fn vertex_auth(extra: &BTreeMap<String, String>) -> Result<ClientAuth, Error> {
    vertex_auth_with(extra, |name| std::env::var(name).ok())
}

/// [`vertex_auth`] over a caller-supplied environment lookup.
/// # Errors
/// Returns an error if the project or Google credentials cannot be resolved.
pub fn vertex_auth_with(
    extra: &BTreeMap<String, String>,
    environment: impl Fn(&str) -> Option<String>,
) -> Result<ClientAuth, Error> {
    let project = extra
        .get("project")
        .cloned()
        .or_else(|| environment("GOOGLE_CLOUD_PROJECT"))
        .filter(|s| !s.is_empty())
        .ok_or(Error::Credentials("Google Cloud project is required"))?;
    let location = extra
        .get("location")
        .cloned()
        .or_else(|| environment("GOOGLE_CLOUD_LOCATION"))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "us-central1".into());
    Ok(ClientAuth::Vertex {
        project,
        location,
        source: Arc::new(GoogleBearerSource::resolve_with(extra, environment)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{DecodingKey, Validation};
    use rsa::{
        RsaPrivateKey,
        pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding},
        rand_core::OsRng,
    };
    use std::sync::atomic::{AtomicI64, Ordering};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    async fn endpoint() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"access_token":"fresh-token","expires_in":3600})),
            )
            .mount(&server)
            .await;
        server
    }

    fn form(request: &wiremock::Request) -> BTreeMap<String, String> {
        reqwest::Url::parse(&format!(
            "http://localhost/?{}",
            String::from_utf8_lossy(&request.body)
        ))
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
    }

    #[tokio::test]
    async fn service_account_assertion_is_rs256_and_cache_expires_early() {
        let server = endpoint().await;
        let key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let private_pem = key.to_pkcs8_pem(LineEnding::LF).unwrap();
        let public_pem = key
            .to_public_key()
            .to_public_key_pem(LineEnding::LF)
            .unwrap();
        let account = json!({"client_email":"test@example.iam.gserviceaccount.com", "private_key":private_pem.as_str(), "token_uri":TOKEN_URL});
        let extra = BTreeMap::from([
            ("service_account_json".into(), account.to_string()),
            ("access_token".into(), "lower-priority".into()),
        ]);
        let mut source =
            GoogleBearerSource::resolve_paths(&extra, Some("/missing".into()), None).unwrap();
        let now = jiff::Timestamp::now().as_second();
        let clock = Arc::new(AtomicI64::new(now));
        let source_clock = clock.clone();
        source.clock = Arc::new(move || source_clock.load(Ordering::SeqCst));
        source.endpoint = format!("{}/token", server.uri());
        assert_eq!(source.token().await.unwrap(), "fresh-token");
        clock.store(now + 3539, Ordering::SeqCst);
        let (first, second) = tokio::join!(source.token(), source.token());
        assert_eq!(first.unwrap(), second.unwrap());
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        clock.store(now + 3540, Ordering::SeqCst);
        source.token().await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 2);
        let form = form(&requests[0]);
        assert_eq!(
            form["grant_type"],
            "urn:ietf:params:oauth:grant-type:jwt-bearer"
        );
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[TOKEN_URL]);
        let decoded = jsonwebtoken::decode::<Value>(
            &form["assertion"],
            &DecodingKey::from_rsa_pem(public_pem.as_bytes()).unwrap(),
            &validation,
        )
        .unwrap();
        assert_eq!(decoded.header.alg, Algorithm::RS256);
        assert_eq!(
            decoded.claims,
            json!({"iss":"test@example.iam.gserviceaccount.com","aud":TOKEN_URL,"scope":SCOPE,"iat":now,"exp":now+3600})
        );
        assert_eq!(
            requests[0].headers["user-agent"],
            concat!("swarmy/", env!("CARGO_PKG_VERSION"))
        );
    }

    #[tokio::test]
    async fn gcloud_default_and_environment_files_refresh() {
        let server = endpoint().await;
        let directory = tempfile::tempdir().unwrap();
        let default = directory
            .path()
            .join("application_default_credentials.json");
        let environment = directory.path().join("environment.json");
        for (file, refresh) in [
            (&default, "default-refresh"),
            (&environment, "environment-refresh"),
        ] {
            std::fs::write(file, json!({"type":"authorized_user","client_id":"client","client_secret":"secret","refresh_token":refresh}).to_string()).unwrap();
        }
        for (path, expected) in [
            (None, "default-refresh"),
            (Some(environment), "environment-refresh"),
        ] {
            let mut source =
                GoogleBearerSource::resolve_paths(&BTreeMap::new(), path, Some(default.clone()))
                    .unwrap();
            source.endpoint = format!("{}/token", server.uri());
            assert_eq!(source.token().await.unwrap(), "fresh-token");
            let requests = server.received_requests().await.unwrap();
            assert_eq!(
                form(requests.last().unwrap()),
                BTreeMap::from([
                    ("grant_type".into(), "refresh_token".into()),
                    ("client_id".into(), "client".into()),
                    ("client_secret".into(), "secret".into()),
                    ("refresh_token".into(), expected.into()),
                ])
            );
        }
        assert!(
            GoogleBearerSource::resolve_paths(
                &BTreeMap::new(),
                Some("/missing".into()),
                Some(default)
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn explicit_access_token_precedes_files_and_builds_shared_vertex_auth() {
        let extra = BTreeMap::from([
            ("access_token".into(), "explicit".into()),
            ("project".into(), "project".into()),
            ("location".into(), "global".into()),
        ]);
        let source =
            GoogleBearerSource::resolve_paths(&extra, Some("/missing".into()), None).unwrap();
        assert_eq!(source.token().await.unwrap(), "explicit");
        let ClientAuth::Vertex {
            project,
            location,
            source,
        } = vertex_auth(&extra).unwrap()
        else {
            panic!("wrong auth variant")
        };
        assert_eq!(project, "project");
        assert_eq!(location, "global");
        assert_eq!(source.token().await.unwrap(), "explicit");
    }

    #[tokio::test]
    async fn refresh_failures_are_not_cached() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(400).set_body_string("sensitive error body"))
            .expect(2)
            .mount(&server)
            .await;
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("adc.json");
        std::fs::write(
            &file,
            json!({"client_id":"id","client_secret":"secret","refresh_token":"refresh"})
                .to_string(),
        )
        .unwrap();
        let mut source =
            GoogleBearerSource::resolve_paths(&BTreeMap::new(), Some(file), None).unwrap();
        source.endpoint = server.uri();
        for _ in 0..2 {
            let error = source.token().await.unwrap_err();
            assert!(matches!(error, Error::Status(_)));
            assert!(!error.to_string().contains("sensitive"));
        }
    }
}
