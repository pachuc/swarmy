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
async fn import_preserves_all_json_and_does_not_modify_source() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("codex-fixture.json");
    let destination = directory.path().join("swarmy/auth.json");
    let bytes = include_bytes!("fixtures/auth.json");
    tokio::fs::write(&source, bytes).await.unwrap();
    let store = FileCredentialStore::new(&destination);
    store.import(&source).await.unwrap();
    assert_eq!(store.load().await.unwrap().to_json(), &fixture());
    assert_eq!(
        serde_json::from_slice::<Value>(&tokio::fs::read(&destination).await.unwrap()).unwrap(),
        fixture()
    );
    assert_eq!(tokio::fs::read(source).await.unwrap(), bytes);
    assert!(store.import(&destination).await.is_err());
    assert_private(&destination);
}

fn assert_private(path: &std::path::Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
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
async fn account_cannot_change_on_disk_or_in_a_live_store() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("auth.json");
    let store = FileCredentialStore::new(&path);
    store.save(credentials()).await.unwrap();
    let mut different = fixture();
    different["tokens"]["id_token"] = json!(jwt("other-account"));
    different["tokens"]["account_id"] = json!("other-account");
    let different = Credentials::from_json(different).unwrap();
    assert!(
        FileCredentialStore::new(&path)
            .save(different.clone())
            .await
            .is_err()
    );
    assert!(store.save(different.clone()).await.is_err());
    assert_eq!(store.load().await.unwrap().to_json(), &fixture());
    tokio::fs::write(path, serde_json::to_vec(different.to_json()).unwrap())
        .await
        .unwrap();
    assert!(store.load().await.is_err());
}

async fn refresh_mock(server: &MockServer) {
    Mock::given(method("POST")).and(path("/oauth/token"))
        .and(body_json(json!({"grant_type": "refresh_token", "client_id": CLIENT_ID, "refresh_token": "refresh-fixture"})))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(40)).set_body_json(json!({"access_token": "access-new", "refresh_token": "refresh-new", "id_token": jwt("account-test")})))
        .expect(1).mount(server).await;
}

#[tokio::test]
async fn concurrent_refresh_is_one_request_and_atomic_private_replacement() {
    let server = MockServer::start().await;
    refresh_mock(&server).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("auth.json");
    let store = FileCredentialStore::new(&path);
    store.save(credentials()).await.unwrap();
    // A separate store and client exercise the account-wide lock registry.
    let other_store = FileCredentialStore::new(&path);
    let oauth = OAuthClient::with_issuer(&server.uri()).unwrap();
    let other_oauth = OAuthClient::with_issuer(&server.uri()).unwrap();
    let observed = store.load().await.unwrap();
    let original_file = std::fs::File::open(&path).unwrap();
    let reads = async {
        for _ in 0..30 {
            let value = FileCredentialStore::new(&path).load().await.unwrap();
            assert!(["access-fixture", "access-new"].contains(&value.access_token()));
            assert_private(&path);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    let (first, second, ()) = tokio::join!(
        oauth.refresh(&store, &observed),
        other_oauth.refresh(&other_store, &observed),
        reads
    );
    let first = first.unwrap();
    assert_eq!(first.to_json(), second.unwrap().to_json());
    assert_eq!(first.access_token(), "access-new");
    let saved = store.load().await.unwrap();
    assert_eq!(saved.to_json()["tokens"]["refresh_token"], "refresh-new");
    assert_ne!(saved.to_json()["last_refresh"], fixture()["last_refresh"]);
    assert_eq!(
        saved.to_json()["future_root_field"],
        fixture()["future_root_field"]
    );
    assert_eq!(
        saved.to_json()["tokens"]["future_token_field"],
        fixture()["tokens"]["future_token_field"]
    );
    // An already-open reader still sees the complete old inode after rename.
    assert_eq!(
        serde_json::from_reader::<_, Value>(original_file).unwrap(),
        fixture()
    );
    assert_private(&path);
    server.verify().await;
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
        let directory = tempfile::tempdir().unwrap();
        let store = FileCredentialStore::new(directory.path().join("auth.json"));
        store.save(credentials()).await.unwrap();
        let oauth = OAuthClient::with_issuer(&server.uri()).unwrap();
        assert!(oauth.refresh(&store, &credentials()).await.is_err());
        assert_eq!(store.load().await.unwrap().to_json(), &fixture());
    }
}

#[tokio::test]
async fn device_code_exchange_persists_codex_layout() {
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
    let code = oauth.device_code().await.unwrap();
    assert_eq!(code.user_code, "ABCD-1234");
    assert_eq!(
        code.verification_url,
        format!("{}/codex/device", server.uri())
    );
    let directory = tempfile::tempdir().unwrap();
    let store = FileCredentialStore::new(directory.path().join("auth.json"));
    oauth.complete_login(code, &store).await.unwrap();
    let saved = store.load().await.unwrap();
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
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(FileCredentialStore::new(directory.path().join("auth.json")));
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

// Executed as a separate process by the test below to bypass the async lock registry.
#[tokio::test]
async fn refresh_process_helper() {
    let Some(path) = std::env::var_os("SWARMY_TEST_REFRESH_FILE") else {
        return;
    };
    let issuer = std::env::var("SWARMY_TEST_REFRESH_ISSUER").unwrap();
    let store = FileCredentialStore::new(path);
    let oauth = OAuthClient::with_issuer(&issuer).unwrap();
    let refreshed = oauth.refresh(&store, &credentials()).await.unwrap();
    assert_eq!(refreshed.access_token(), "access-new");
}

#[tokio::test]
async fn separate_processes_share_the_refresh_file_lock() {
    let server = MockServer::start().await;
    refresh_mock(&server).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("auth.json");
    FileCredentialStore::new(&path)
        .save(credentials())
        .await
        .unwrap();
    let process = || {
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "refresh_process_helper"])
            .env("SWARMY_TEST_REFRESH_FILE", &path)
            .env("SWARMY_TEST_REFRESH_ISSUER", server.uri());
        command
    };
    let (first, second) = tokio::join!(process().output(), process().output());
    for output in [first.unwrap(), second.unwrap()] {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    server.verify().await;
}

#[tokio::test]
async fn provider_accepts_streams_without_a_content_type_header() {
    let server = MockServer::start().await;
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(FileCredentialStore::new(directory.path().join("auth.json")));
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
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(FileCredentialStore::new(directory.path().join("auth.json")));
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
