use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::TryStreamExt;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use swarmy_llm::{
    Delta, GenerationSettings, Provider, Request,
    auth::{CLIENT_ID, CredentialStore, Credentials, FileCredentialStore, OAuthClient},
    chatgpt::ChatGptProvider,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method, path},
};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/auth.json")).unwrap()
}
fn credentials() -> Credentials {
    Credentials::from_json(fixture()).unwrap()
}
fn jwt(account: &str) -> String {
    format!(
        "test.{}.test",
        URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(
                &json!({"https://api.openai.com/auth": {"chatgpt_account_id": account}})
            )
            .unwrap()
        )
    )
}
fn request() -> Request {
    Request {
        system_prompt: "Be brief.".into(),
        messages: vec![],
        tools: vec![],
        settings: GenerationSettings {
            model: "fixture-model".into(),
            ..Default::default()
        },
    }
}

#[tokio::test]
async fn import_preserves_all_json_and_file_is_read_only() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("auth.json");
    let bytes = include_bytes!("fixtures/auth.json");
    tokio::fs::write(&source, bytes).await.unwrap();
    let store = FileCredentialStore::new(&source);
    let loaded = store.load().await.unwrap();
    assert_eq!(loaded.to_json(), &fixture());
    assert_eq!(
        Credentials::from_record(&loaded.to_record().unwrap())
            .unwrap()
            .to_json(),
        &fixture()
    );
    assert!(
        OAuthClient::new()
            .unwrap()
            .refresh(&store, &loaded)
            .await
            .is_err()
    );
    assert_eq!(tokio::fs::read(source).await.unwrap(), bytes);
}

#[test]
fn reject_api_keys_and_inconsistent_account_claims() {
    let mut value = fixture();
    value["OPENAI_API_KEY"] = json!("not-an-accepted-key");
    assert!(Credentials::from_json(value).is_err());
    let mut value = fixture();
    value["tokens"]["account_id"] = json!("another-account");
    assert!(Credentials::from_json(value).is_err());
    assert!(
        Credentials::from_json(json!({"auth_mode": "apikey", "OPENAI_API_KEY": "fixture"}))
            .is_err()
    );
}

#[tokio::test]
async fn account_cannot_change_on_disk() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("auth.json");
    std::fs::write(&path, serde_json::to_vec(&fixture()).unwrap()).unwrap();
    let store = FileCredentialStore::new(&path);
    store.load().await.unwrap();
    let mut different = fixture();
    different["tokens"]["id_token"] = json!(jwt("other-account"));
    different["tokens"]["account_id"] = json!("other-account");
    std::fs::write(path, serde_json::to_vec(&different).unwrap()).unwrap();
    assert!(store.load().await.is_err());
}

async fn refresh_mock(server: &MockServer) {
    Mock::given(method("POST")).and(path("/oauth/token"))
        .and(body_json(json!({"grant_type": "refresh_token", "client_id": CLIENT_ID, "refresh_token": "refresh-fixture"})))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(40)).set_body_json(json!({"access_token": "access-new", "refresh_token": "refresh-new", "id_token": jwt("account-test")})))
        .expect(1).mount(server).await;
}

#[tokio::test]
async fn failed_refresh_or_account_change_does_not_replace_credentials() {
    for response in [
        ResponseTemplate::new(401),
        ResponseTemplate::new(200)
            .set_body_json(json!({"access_token": "new", "id_token": jwt("other-account")})),
    ] {
        let server = MockServer::start().await;
        Mock::given(path("/oauth/token"))
            .respond_with(response)
            .expect(1)
            .mount(&server)
            .await;
        let store = MemoryCredentials::default();
        store.save(credentials()).await.unwrap();
        let oauth = OAuthClient::with_issuer(&server.uri()).unwrap();
        assert!(oauth.refresh(&store, &credentials()).await.is_err());
        assert_eq!(store.load().await.unwrap().to_json(), &fixture());
    }
}

#[tokio::test]
async fn device_code_exchange_persists_codex_layout() {
    struct Ui;
    #[async_trait::async_trait]
    impl swarmy_llm::auth::LoginUi for Ui {
        async fn notify_device_code(&self, url: &str, code: &str) -> Result<(), swarmy_llm::Error> {
            assert_eq!(code, "ABCD-1234");
            assert!(url.ends_with("/codex/device"));
            Ok(())
        }
        async fn notify_url(&self, _: &str) -> Result<(), swarmy_llm::Error> {
            unreachable!()
        }
        async fn prompt_secret(&self, _: &str) -> Result<String, swarmy_llm::Error> {
            unreachable!()
        }
        async fn prompt_choice(&self, _: &str, _: &[&str]) -> Result<usize, swarmy_llm::Error> {
            unreachable!()
        }
    }
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/accounts/deviceauth/usercode"))
        .and(body_json(json!({"client_id": CLIENT_ID})))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"device_auth_id":"device-fixture", "user_code":"ABCD-1234", "interval":"1"}),
        ))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST")).and(path("/api/accounts/deviceauth/token"))
        .and(body_json(json!({"device_auth_id":"device-fixture", "user_code":"ABCD-1234"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"authorization_code":"approved-code", "code_verifier":"pkce-verifier", "code_challenge":"pkce-challenge"})))
        .expect(1).mount(&server).await;
    Mock::given(method("POST")).and(path("/oauth/token"))
        .and(header("content-type", "application/x-www-form-urlencoded"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id_token":jwt("account-test"), "access_token":"login-access", "refresh_token":"login-refresh"})))
        .expect(1).mount(&server).await;
    let oauth = OAuthClient::with_issuer(&server.uri()).unwrap();
    let kind = swarmy_llm::auth::Login::login(&oauth, &Ui).await.unwrap();
    let swarmy_core::CredentialKind::OAuth { extra, .. } = &kind else {
        panic!("expected OAuth");
    };
    assert_eq!(extra["account_id"], "account-test");
    let saved = Credentials::from_record(&swarmy_core::CredentialRecord {
        kind,
        updated_at: jiff::Timestamp::now(),
    })
    .unwrap();
    assert_eq!(saved.account_id(), "account-test");
    assert_eq!(saved.access_token(), "login-access");
    assert_eq!(saved.to_json()["auth_mode"], "chatgpt");
    assert!(saved.to_json()["OPENAI_API_KEY"].is_null());
    let requests = server.received_requests().await.unwrap();
    let exchange = requests.last().unwrap();
    let body = std::str::from_utf8(&exchange.body).unwrap();
    for field in [
        "grant_type=authorization_code",
        "code=approved-code",
        "code_verifier=pkce-verifier",
        "client_id=app_EMoamEEZ73f0CkXaXp7hrann",
        "redirect_uri=",
    ] {
        assert!(body.contains(field));
    }
}

#[tokio::test]
async fn provider_reloads_store_and_retries_unauthorized_once() {
    let server = MockServer::start().await;
    let store = Arc::new(MemoryCredentials::default());
    let mut initial = fixture();
    initial["last_refresh"] = json!(jiff::Timestamp::now().to_string());
    store
        .save(Credentials::from_json(initial.clone()).unwrap())
        .await
        .unwrap();
    Mock::given(path("/responses"))
        .and(header("authorization", "Bearer access-fixture"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;
    refresh_mock(&server).await;
    for token in ["access-new", "access-external"] {
        Mock::given(method("POST"))
            .and(path("/responses"))
            .and(header("authorization", format!("Bearer {token}")))
            .and(header("chatgpt-account-id", "account-test"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(include_str!("fixtures/text.sse"), "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    let provider: Box<dyn Provider> = Box::new(
        ChatGptProvider::with_endpoints(
            store.clone(),
            &server.uri(),
            OAuthClient::with_issuer(&server.uri()).unwrap(),
        )
        .unwrap(),
    );
    let events: Vec<_> = provider.request(request()).try_collect().await.unwrap();
    assert!(matches!(events.last(), Some(Delta::Completed(_))));
    initial["tokens"]["access_token"] = json!("access-external");
    store
        .save(Credentials::from_json(initial).unwrap())
        .await
        .unwrap();
    provider
        .request(request())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let requests = server.received_requests().await.unwrap();
    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["stream"], true);
    assert_eq!(body["store"], false);
    server.verify().await;
}

#[tokio::test]
async fn provider_accepts_streams_without_a_content_type_header() {
    let server = MockServer::start().await;
    let store = Arc::new(MemoryCredentials::default());
    let mut initial = fixture();
    initial["last_refresh"] = json!(jiff::Timestamp::now().to_string());
    store
        .save(Credentials::from_json(initial).unwrap())
        .await
        .unwrap();
    // The live Codex backend sends its event stream with no Content-Type at all.
    Mock::given(method("POST"))
        .and(path("/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(include_str!("fixtures/text.sse")))
        .expect(2)
        .mount(&server)
        .await;
    let provider: Box<dyn Provider> = Box::new(
        ChatGptProvider::with_endpoints(
            store,
            &server.uri(),
            OAuthClient::with_issuer(&server.uri()).unwrap(),
        )
        .unwrap(),
    );
    let events: Vec<_> = provider.request(request()).try_collect().await.unwrap();
    assert!(matches!(events.last(), Some(Delta::Completed(_))));
    let again: Vec<_> = provider.request(request()).try_collect().await.unwrap();
    assert_eq!(again.len(), events.len());
    server.verify().await;
}

#[tokio::test]
async fn provider_rejects_an_explicit_non_stream_content_type() {
    let server = MockServer::start().await;
    let store = Arc::new(MemoryCredentials::default());
    let mut initial = fixture();
    initial["last_refresh"] = json!(jiff::Timestamp::now().to_string());
    store
        .save(Credentials::from_json(initial).unwrap())
        .await
        .unwrap();
    Mock::given(method("POST"))
        .and(path("/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_raw("<html>blocked</html>", "text/html"))
        .mount(&server)
        .await;
    let provider = ChatGptProvider::with_endpoints(
        store,
        &server.uri(),
        OAuthClient::with_issuer(&server.uri()).unwrap(),
    )
    .unwrap();
    let error = provider
        .request(request())
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("got text/html"), "{error}");
}

#[test]
fn chatgpt_record_round_trip_preserves_metadata() {
    let credentials = swarmy_llm::auth::Credentials::from_json(
        serde_json::from_str(include_str!("fixtures/auth.json")).unwrap(),
    )
    .unwrap();
    let record = credentials.to_record().unwrap();
    let reconstructed = swarmy_llm::auth::Credentials::from_record(&record).unwrap();
    assert_eq!(credentials.to_json(), reconstructed.to_json());
    let swarmy_core::CredentialKind::OAuth {
        access, refresh, ..
    } = record.kind
    else {
        panic!("expected OAuth");
    };
    assert_eq!(access, credentials.access_token());
    assert!(!refresh.is_empty());
}

// Protocol fixtures use memory; production refresh ownership is tested against FDB.
#[derive(Default)]
struct MemoryCredentials(tokio::sync::Mutex<Option<Credentials>>);
impl MemoryCredentials {
    async fn save(&self, credentials: Credentials) -> Result<(), swarmy_llm::Error> {
        *self.0.lock().await = Some(credentials);
        Ok(())
    }
}
impl CredentialStore for MemoryCredentials {
    fn load(&self) -> futures::future::BoxFuture<'_, Result<Credentials, swarmy_llm::Error>> {
        Box::pin(async { Ok(self.0.lock().await.clone().unwrap()) })
    }
    fn refresh<'a>(
        &'a self,
        client: &'a OAuthClient,
        observed: &'a Credentials,
    ) -> futures::future::BoxFuture<'a, Result<Credentials, swarmy_llm::Error>> {
        Box::pin(async move {
            let mut stored = self.0.lock().await;
            let current = stored.clone().unwrap();
            if current != *observed {
                return Ok(current);
            }
            let updated = client.refresh_credentials(current).await?;
            *stored = Some(updated.clone());
            Ok(updated)
        })
    }
}

#[tokio::test]
async fn native_store_records_need_only_account_metadata() {
    let now = jiff::Timestamp::now();
    let record = swarmy_core::CredentialRecord {
        kind: swarmy_core::CredentialKind::OAuth {
            access: "access-fixture".into(),
            refresh: "refresh-fixture".into(),
            expires_at: now,
            extra: [("account_id".into(), "account-test".into())].into(),
        },
        updated_at: now,
    };
    let credentials = Credentials::from_record(&record).unwrap();
    assert_eq!(credentials.account_id(), "account-test");
    let server = MockServer::start().await;
    refresh_mock(&server).await;
    let refreshed = OAuthClient::with_issuer(&server.uri())
        .unwrap()
        .refresh_credentials(credentials)
        .await
        .unwrap();
    assert_eq!(refreshed.access_token(), "access-new");
    assert_eq!(refreshed.account_id(), "account-test");
}

/// A pinned route step resolves exactly its entry instead of the pool's
/// first choice, and a missing label is an operator error.
#[tokio::test]
async fn pinned_resolution_selects_the_named_entry() {
    use std::collections::BTreeMap;
    use swarmy_llm::auth::{AuthStore, Login, Resolver};

    struct Stub {
        entries: Vec<(String, swarmy_core::CredentialRecord)>,
    }

    fn api_key(key: &str) -> swarmy_core::CredentialRecord {
        swarmy_core::CredentialRecord {
            kind: swarmy_core::CredentialKind::ApiKey {
                key: key.into(),
                extra: BTreeMap::new(),
            },
            updated_at: jiff::Timestamp::now(),
        }
    }

    #[async_trait::async_trait]
    impl AuthStore for Stub {
        async fn get(
            &self,
            _provider: &str,
        ) -> Result<Option<swarmy_core::CredentialRecord>, swarmy_llm::Error> {
            Ok(self.entries.first().map(|(_, record)| record.clone()))
        }

        async fn get_labelled(
            &self,
            _provider: &str,
        ) -> Result<Option<(Option<String>, swarmy_core::CredentialRecord)>, swarmy_llm::Error>
        {
            Ok(self
                .entries
                .first()
                .map(|(label, record)| (Some(label.clone()), record.clone())))
        }

        async fn get_exact(
            &self,
            _provider: &str,
            label: &str,
        ) -> Result<Option<swarmy_core::CredentialRecord>, swarmy_llm::Error> {
            Ok(self
                .entries
                .iter()
                .find(|(entry, _)| entry == label)
                .map(|(_, record)| record.clone()))
        }

        async fn refresh(
            &self,
            _provider: &str,
            observed: &swarmy_core::CredentialRecord,
            _login: &dyn Login,
        ) -> Result<swarmy_core::CredentialRecord, swarmy_llm::Error> {
            Ok(observed.clone())
        }
    }

    let store = Arc::new(Stub {
        entries: vec![
            ("primary".into(), api_key("primary-key")),
            ("backup".into(), api_key("backup-key")),
        ],
    });
    let resolver = Resolver::new(store).unwrap();
    // The pool serves the first entry.
    assert_eq!(
        resolver.resolve("openai").await.unwrap().entry.as_deref(),
        Some("primary"),
    );
    // The pinned step serves exactly its entry, even when it is not first.
    let pinned = resolver.resolve_pinned("openai", "backup").await.unwrap();
    assert_eq!(pinned.entry.as_deref(), Some("backup"));
    let swarmy_llm::ClientAuth::ApiKey(key) = pinned.auth else {
        panic!("expected an API key client");
    };
    assert_eq!(key, "backup-key");
    assert!(resolver.resolve_pinned("openai", "missing").await.is_err());
}
