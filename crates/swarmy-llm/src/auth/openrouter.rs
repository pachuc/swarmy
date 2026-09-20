use super::{Login, LoginUi};
use crate::Error;
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::Duration};
use swarmy_core::CredentialKind;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

pub struct OpenRouterLogin {
    client: reqwest::Client,
    base: String,
}

impl OpenRouterLogin {
    /// # Errors
    /// Returns HTTP client configuration errors.
    pub fn new() -> Result<Self, Error> {
        Self::with_base("https://openrouter.ai")
    }

    /// Override the host for protocol fixtures.
    /// # Errors
    /// Returns HTTP client configuration errors.
    pub fn with_base(base: &str) -> Result<Self, Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent(concat!("swarmy/", env!("CARGO_PKG_VERSION")))
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(30))
                .build()?,
            base: base.trim_end_matches('/').into(),
        })
    }
}

#[async_trait]
impl Login for OpenRouterLogin {
    fn provider(&self) -> &'static str {
        "openrouter"
    }
    async fn login(&self, ui: &dyn LoginUi) -> Result<CredentialKind, Error> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let callback = format!(
            "http://127.0.0.1:{}/callback",
            listener.local_addr()?.port()
        );
        let mut bytes = [0; 32];
        rand::rng().fill_bytes(&mut bytes);
        let verifier = URL_SAFE_NO_PAD.encode(bytes);
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let mut url = url::Url::parse(&format!("{}/auth", self.base))
            .map_err(|_| Error::Credentials("invalid OpenRouter auth URL"))?;
        url.query_pairs_mut().extend_pairs([
            ("callback_url", callback.as_str()),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("key_label", "swarmy"),
        ]);
        ui.notify_url(url.as_str()).await?;
        // Choosing a mode avoids leaving an uncancellable stdin read behind.
        let mode = ui
            .prompt_choice(
                "Receive the authorization code",
                &["Browser callback", "Paste code (headless)"],
            )
            .await?;
        let code = tokio::time::timeout(Duration::from_mins(15), async {
            match mode {
                0 => callback_code(&listener).await,
                1 => ui.prompt_secret("Paste the OpenRouter code").await,
                _ => Err(Error::Credentials("invalid login choice")),
            }
        })
        .await
        .map_err(|_| Error::LoginTimeout)??;
        if code.trim().is_empty() {
            return Err(Error::Credentials("empty OpenRouter code"));
        }
        let response = self.client.post(format!("{}/api/v1/auth/keys", self.base))
            .json(&json!({"code":code.trim(), "code_verifier":verifier, "code_challenge_method":"S256"}))
            .send().await?;
        if !response.status().is_success() {
            return Err(Error::Status(response.status()));
        }
        let value: serde_json::Value = response.json().await?;
        let key = value["key"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(Error::Credentials("OpenRouter returned no key"))?;
        Ok(CredentialKind::ApiKey {
            key: key.into(),
            extra: BTreeMap::new(),
        })
    }
    async fn refresh(&self, _record: &CredentialKind) -> Result<Option<CredentialKind>, Error> {
        Ok(None)
    }
}

async fn callback_code(listener: &TcpListener) -> Result<String, Error> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let mut request = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(5), async {
            let mut byte = [0];
            while request.len() < 8192 && !request.ends_with(b"\r\n\r\n") {
                if stream.read(&mut byte).await? == 0 {
                    break;
                }
                request.push(byte[0]);
            }
            Ok::<_, std::io::Error>(())
        })
        .await;
        if !matches!(read, Ok(Ok(()))) {
            continue;
        }
        let text = String::from_utf8_lossy(&request);
        let code = text
            .lines()
            .next()
            .and_then(|line| line.strip_prefix("GET "))
            .and_then(|line| line.split_whitespace().next())
            .filter(|target| target.starts_with("/callback?"))
            .and_then(|target| url::Url::parse(&format!("http://localhost{target}")).ok())
            .and_then(|url| {
                url.query_pairs()
                    .find(|(key, value)| key == "code" && !value.is_empty())
                    .map(|(_, value)| value.into_owned())
            });
        let reply: &[u8] = if code.is_some() {
            b"HTTP/1.1 200 OK\r\nContent-Length: 22\r\nConnection: close\r\n\r\nReturn to swarmy login."
        } else {
            b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        };
        let _ = stream.write_all(reply).await;
        if let Some(code) = code {
            return Ok(code);
        }
    }
}
