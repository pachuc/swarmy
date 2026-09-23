use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};
use std::sync::Mutex;
use swarmy_core::CredentialKind;
use swarmy_llm::{
    Error,
    auth::{AzureLogin, Login, LoginUi, OpenRouterLogin},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

struct Ui {
    url: Mutex<String>,
    callback: bool,
}

#[async_trait]
impl LoginUi for Ui {
    async fn notify_url(&self, url: &str) -> Result<(), Error> {
        *self.url.lock().unwrap() = url.into();
        if self.callback {
            let url = url::Url::parse(url).unwrap();
            let (_, callback) = url
                .query_pairs()
                .find(|(key, _)| key == "callback_url")
                .unwrap();
            let callback = callback.into_owned();
            tokio::spawn(async move {
                reqwest::get(format!("{callback}?code=approved-code"))
                    .await
                    .unwrap();
            });
        }
        Ok(())
    }
    async fn notify_device_code(&self, _url: &str, _code: &str) -> Result<(), Error> {
        unreachable!()
    }
    async fn prompt_secret(&self, _prompt: &str) -> Result<String, Error> {
        Ok("approved-code".into())
    }
    async fn prompt_choice(&self, _prompt: &str, _choices: &[&str]) -> Result<usize, Error> {
        Ok(usize::from(!self.callback))
    }
}

#[tokio::test]
async fn openrouter_pkce_exchanges_verifier_for_api_key_in_both_modes() {
    for callback in [false, true] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/auth/keys"))
            .and(header(
                "user-agent",
                concat!("swarmy/", env!("CARGO_PKG_VERSION")),
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"key":"minted-key"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let ui = Ui {
            url: Mutex::new(String::new()),
            callback,
        };
        let login = OpenRouterLogin::with_base(&server.uri()).unwrap();
        let kind = login.login(&ui).await.unwrap();
        assert!(matches!(&kind, CredentialKind::ApiKey { key, .. } if key == "minted-key"));
        assert!(login.refresh(&kind).await.unwrap().is_none());
        let url = url::Url::parse(&ui.url.lock().unwrap()).unwrap();
        let params: std::collections::BTreeMap<_, _> = url.query_pairs().collect();
        assert_eq!(params["key_label"], "swarmy");
        assert_eq!(params["code_challenge_method"], "S256");
        assert!(!params.contains_key("client_id"));
        let callback = url::Url::parse(&params["callback_url"]).unwrap();
        assert_eq!(callback.host_str(), Some("127.0.0.1"));
        assert_eq!(callback.path(), "/callback");
        let requests = server.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["code"], "approved-code");
        assert_eq!(body["code_challenge_method"], "S256");
        let verifier = body["code_verifier"].as_str().unwrap();
        assert_eq!(URL_SAFE_NO_PAD.decode(verifier).unwrap().len(), 32);
        assert_eq!(
            params["code_challenge"],
            URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
        );
    }
}

#[tokio::test]
async fn azure_process_helper() {
    let Ok(mode) = std::env::var("SWARMY_TEST_AZURE_MODE") else {
        return;
    };
    let ui = Ui {
        url: Mutex::new(String::new()),
        callback: false,
    };
    let login = AzureLogin::new("test-resource", Some("https://custom/.default"));
    let result = login.login(&ui).await;
    if mode == "missing" {
        assert!(matches!(result, Err(Error::NeedsLogin(provider)) if provider == "azure"));
        return;
    }
    let kind = result.unwrap();
    let CredentialKind::OAuth {
        access,
        refresh,
        expires_at,
        extra,
    } = &kind
    else {
        panic!("expected OAuth");
    };
    assert_eq!(access, "az-fixture-token");
    assert!(refresh.is_empty());
    assert_eq!(expires_at.to_string(), "2099-01-02T03:04:05Z");
    assert_eq!(extra["resource_name"], "test-resource");
    assert_eq!(extra["scope"], "https://custom/.default");
    let record = swarmy_core::CredentialRecord {
        kind: kind.clone(),
        updated_at: jiff::Timestamp::now(),
    };
    assert_eq!(
        record.status(jiff::Timestamp::now()),
        swarmy_core::CredentialStatus::Ready
    );
    assert!(login.refresh(&kind).await.unwrap() == Some(kind));
    let foundry = AzureLogin::new(
        "https://foundry.services.ai.azure.com",
        Some("https://custom/.default"),
    );
    let foundry_kind = foundry.login(&ui).await.unwrap();
    let CredentialKind::OAuth { extra, .. } = &foundry_kind else {
        panic!("expected OAuth");
    };
    assert_eq!(extra["base_url"], "https://foundry.services.ai.azure.com");
    assert!(foundry.refresh(&foundry_kind).await.unwrap() == Some(foundry_kind));
}

#[test]
#[cfg(unix)]
fn azure_cli_parses_expiry_preserves_scope_and_reports_missing_az() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("az");
    std::fs::write(
        &script,
        r#"#!/bin/sh
[ "$*" = 'account get-access-token --scope https://custom/.default --output json' ] || exit 1
printf '%s\n' '{"accessToken":"az-fixture-token","expiresOn":"2099-01-02 03:04:05.000000"}'
"#,
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    for mode in ["present", "missing"] {
        if mode == "missing" {
            std::fs::remove_file(&script).unwrap();
        }
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "azure_process_helper", "--nocapture"])
            .env("SWARMY_TEST_AZURE_MODE", mode)
            .env("PATH", dir.path())
            .env("TZ", "UTC")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }
}
