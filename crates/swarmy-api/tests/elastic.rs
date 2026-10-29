#![deny(clippy::disallowed_methods)]
use std::sync::Arc;
use swarmy_api::{AppState, router};
use swarmy_api_types::{
    CreateCredential, CredentialKind, ElasticGroup, PutElasticGroup, SetElasticDesired,
};
use swarmy_bus::{Bus, Config};
use swarmy_config::Keyring;
use swarmy_store::blob::MemoryBlobStore;

#[tokio::test]
async fn group_routes_and_machine_credentials() {
    let Some(stack) = swarmy_testkit::Stack::load("api-elastic") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    let bus = Bus::connect(&stack.nats_url, Config::default())
        .await
        .unwrap();
    let mut state = AppState::new(
        store,
        bus,
        "test-token".into(),
        swarmy_llm::catalog::Catalog::get().clone(),
        Arc::new(object_store::memory::InMemory::new()),
    );
    state.credential_keyring = Some(Keyring::from_bytes([7; 32]));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let task = tokio::spawn(axum::serve(listener, router(state)).into_future());
    let client = reqwest::Client::new();
    assert_registration(&client, &base).await;
    assert_desired(&client, &base).await;
    assert_bad_definitions(&client, &base).await;
    assert_machine_credentials(&client, &base).await;
    task.abort();
}

fn group_body(key: &str) -> PutElasticGroup {
    PutElasticGroup {
        idempotency_key: key.into(),
        swarm: "fleet".into(),
        module: "hetzner-cloud".into(),
        size: "cpx41".into(),
        location: "fsn1".into(),
        sandboxes: 64,
        local_storage: "dir:/srv/swarmy-local".into(),
        max: 2,
        price_per_hour: "0.75".into(),
        currency: "EUR".into(),
        primary_address: "192.0.2.1".into(),
        service_user: "swarmy".into(),
    }
}

async fn assert_api_error(
    response: reqwest::Response,
    status: u16,
    code: &str,
) -> serde_json::Value {
    assert_eq!(response.status().as_u16(), status);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["code"], code);
    body
}

async fn assert_registration(client: &reqwest::Client, base: &str) {
    let group_url = format!("{base}/swarm/groups/gpu");
    let response = client
        .get(&group_url)
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    let response = client
        .put(&group_url)
        .bearer_auth("test-token")
        .json(&group_body("register"))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let registered: ElasticGroup = response.json().await.unwrap();
    assert_eq!(registered.price_per_hour, "0.75");
    assert_eq!(registered.desired, 0);
    assert_eq!(
        client
            .get(&group_url)
            .bearer_auth("test-token")
            .send()
            .await
            .unwrap()
            .json::<ElasticGroup>()
            .await
            .unwrap(),
        registered
    );
}

async fn assert_desired(client: &reqwest::Client, base: &str) {
    let group_url = format!("{base}/swarm/groups/gpu");
    let desired_url = format!("{group_url}/desired");
    let desired = SetElasticDesired {
        idempotency_key: "desired-one".into(),
        desired: 1,
        reason: None,
    };
    let changed: ElasticGroup = client
        .put(&desired_url)
        .bearer_auth("test-token")
        .json(&desired)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (changed.desired, changed.generation, changed.settled),
        (1, 1, false)
    );
    let desired_two = SetElasticDesired {
        idempotency_key: "desired-two".into(),
        desired: 2,
        reason: None,
    };
    let latest: ElasticGroup = client
        .put(&desired_url)
        .bearer_auth("test-token")
        .json(&desired_two)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!((latest.desired, latest.generation), (2, 2));
    assert_eq!(
        client
            .put(&desired_url)
            .bearer_auth("test-token")
            .json(&desired)
            .send()
            .await
            .unwrap()
            .json::<ElasticGroup>()
            .await
            .unwrap(),
        changed
    );
    let current: ElasticGroup = client
        .get(&group_url)
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!((current.desired, current.generation), (2, 2));
    let too_many = SetElasticDesired {
        idempotency_key: "desired-many".into(),
        desired: 3,
        reason: None,
    };
    let rejected = client
        .put(&desired_url)
        .bearer_auth("test-token")
        .json(&too_many)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 400);
    assert_eq!(
        rejected.json::<serde_json::Value>().await.unwrap()["code"],
        "desired_above_max"
    );
}

async fn assert_bad_definitions(client: &reqwest::Client, base: &str) {
    let group_url = format!("{base}/swarm/groups/gpu");
    let mut busy = group_body("busy");
    busy.size = "different".into();
    assert_api_error(
        client
            .put(&group_url)
            .bearer_auth("test-token")
            .json(&busy)
            .send()
            .await
            .unwrap(),
        409,
        "elastic_group_busy",
    )
    .await;
    let mut invalid = group_body("invalid");
    invalid.module = "existing".into();
    assert_api_error(
        client
            .put(&group_url)
            .bearer_auth("test-token")
            .json(&invalid)
            .send()
            .await
            .unwrap(),
        400,
        "invalid_elastic_group",
    )
    .await;
    let missing = client
        .put(format!("{base}/swarm/groups/missing/desired"))
        .bearer_auth("test-token")
        .json(&SetElasticDesired {
            idempotency_key: "missing".into(),
            desired: 1,
            reason: None,
        })
        .send()
        .await
        .unwrap();
    assert_api_error(missing, 404, "elastic_group_not_found").await;
}

async fn assert_machine_credentials(client: &reqwest::Client, base: &str) {
    let cred = CreateCredential {
        idempotency_key: "machine-key".into(),
        provider: "hetzner-cloud".into(),
        kind: CredentialKind::MachineProvider,
        label: "default".into(),
        secret: String::new(),
        extra: [("HCLOUD_TOKEN".into(), "example-secret".into())].into(),
    };
    let response = client
        .post(format!("{base}/credentials"))
        .bearer_auth("test-token")
        .json(&cred)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["kind"],
        "machine_provider"
    );
    let mut unknown = cred.clone();
    unknown.idempotency_key = "unknown-provider".into();
    unknown.provider = "openai".into();
    assert_api_error(
        client
            .post(format!("{base}/credentials"))
            .bearer_auth("test-token")
            .json(&unknown)
            .send()
            .await
            .unwrap(),
        400,
        "unknown_machine_provider",
    )
    .await;
    let mut absent = cred;
    absent.idempotency_key = "missing-token".into();
    absent.extra.clear();
    let missing = assert_api_error(
        client
            .post(format!("{base}/credentials"))
            .bearer_auth("test-token")
            .json(&absent)
            .send()
            .await
            .unwrap(),
        400,
        "invalid_machine_credential",
    )
    .await;
    let message = missing["message"].as_str().unwrap();
    assert!(message.contains("HCLOUD_TOKEN"));
    assert!(!message.contains("example-secret"));
}
